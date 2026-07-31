package lease

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// mongoLeaseManager 基于 MongoDB 的租约管理器实现。
//
// 使用 findOneAndUpdate + insertOne 组合实现原子 CAS 抢锁。
// 租约数据存储在 MongoDB 集合中，每条文档包含 key、owner、generation、updated_at 字段。
//
// 集合文档结构：
//
//	{
//	  "_id": ObjectId,
//	  "key": "player:123",       // 唯一索引
//	  "owner": "node-3",
//	  "generation": NumberLong(1),
//	  "updated_at": ISODate(...)
//	}
//
// 需要在 key 字段上创建唯一索引：
//
//	db.leases.createIndex({key: 1}, {unique: true})
//
// 断线重连：
//
//	MongoDB Go Driver v2 内置连接池管理和服务发现，会在网络中断后自动重连。
//	本实现额外提供了 retryAttempts 参数，对瞬态网络错误（如连接超时、DNS 解析失败）
//	进行指数退避重试。逻辑错误（ErrNotAcquired / ErrLeaseExpired）不重试。
//	Acquire 调用是幂等的：如果 key 已被同一 owner 持有，直接返回现有租约。
type mongoLeaseManager struct {
	col           *mongo.Collection
	leaseTimeout  time.Duration
	retryAttempts int
	mu            sync.Mutex
}

// NewMongoManager 创建一个基于 MongoDB 的租约管理器。
// collection 是用于存储租约的 MongoDB 集合，需要预先在 key 字段上创建唯一索引。
// leaseTimeout 指定租约过期时间：超过此时间未续约的租约可以被其他 owner 抢占。
// retryAttempts 指定瞬态网络错误的最大重试次数（含首次），设为 0 表示不重试。
func NewMongoManager(collection *mongo.Collection, leaseTimeout time.Duration, retryAttempts int) Manager {
	if retryAttempts <= 0 {
		retryAttempts = 1
	}
	return &mongoLeaseManager{
		col:           collection,
		leaseTimeout:  leaseTimeout,
		retryAttempts: retryAttempts,
	}
}

type mongoLeaseDoc struct {
	Key        string    `bson:"key"`
	Owner      string    `bson:"owner"`
	Generation int64     `bson:"generation"`
	UpdatedAt  time.Time `bson:"updated_at"`
}

func (m *mongoLeaseManager) Acquire(ctx context.Context, key, owner string) (*Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var (
		lease *Lease
		err   error
	)

	for attempt := 0; attempt < m.retryAttempts; attempt++ {
		lease, err = m.acquireOnce(ctx, key, owner)
		if err == nil {
			return lease, nil
		}
		// 逻辑错误不重试
		if errors.Is(err, ErrNotAcquired) {
			return nil, err
		}
		// 非瞬态错误不重试
		if !isTransientNetErr(err) {
			return nil, err
		}
		if attempt < m.retryAttempts-1 {
			sleepBackoff(ctx, attempt)
		}
	}
	return lease, err
}

func (m *mongoLeaseManager) acquireOnce(ctx context.Context, key, owner string) (*Lease, error) {
	now := time.Now()
	expireTime := now.Add(-m.leaseTimeout)

	// 过滤条件：空闲、已过期、或已被同一 owner 持有（幂等）
	filter := bson.M{
		"key": key,
		"$or": []bson.M{
			{"owner": ""},
			{"owner": owner},
			{"updated_at": bson.M{"$lt": expireTime}},
		},
	}
	update := bson.M{
		"$set": bson.M{
			"owner":      owner,
			"updated_at": now,
		},
		"$inc": bson.M{"generation": 1},
	}

	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
	var doc mongoLeaseDoc
	err := m.col.FindOneAndUpdate(ctx, filter, update, opts).Decode(&doc)

	if errors.Is(err, mongo.ErrNoDocuments) {
		// 文档不存在，尝试插入新文档
		doc := mongoLeaseDoc{
			Key:        key,
			Owner:      owner,
			Generation: 1,
			UpdatedAt:  now,
		}
		_, err := m.col.InsertOne(ctx, doc)
		if err != nil {
			if mongo.IsDuplicateKeyError(err) {
				// key 已存在且被其他 owner 持有且未过期
				return nil, ErrNotAcquired
			}
			return nil, err
		}
		return &Lease{Key: key, Owner: owner, Generation: 1}, nil
	}
	if err != nil {
		return nil, err
	}

	if doc.Owner != owner {
		return nil, ErrNotAcquired
	}

	return &Lease{
		Key:        doc.Key,
		Owner:      doc.Owner,
		Generation: doc.Generation,
	}, nil
}

func (m *mongoLeaseManager) Release(ctx context.Context, lease *Lease) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var lastErr error
	for attempt := 0; attempt < m.retryAttempts; attempt++ {
		err := m.releaseOnce(ctx, lease)
		if err == nil {
			return nil
		}
		lastErr = err
		if errors.Is(err, ErrLeaseExpired) {
			return err
		}
		if !isTransientNetErr(err) {
			return err
		}
		if attempt < m.retryAttempts-1 {
			sleepBackoff(ctx, attempt)
		}
	}
	return lastErr
}

func (m *mongoLeaseManager) releaseOnce(ctx context.Context, lease *Lease) error {
	filter := bson.M{
		"key":        lease.Key,
		"owner":      lease.Owner,
		"generation": lease.Generation,
	}
	update := bson.M{
		"$set": bson.M{
			"owner":      "",
			"updated_at": time.Now(),
		},
	}

	result, err := m.col.UpdateOne(ctx, filter, update)
	if err != nil {
		return err
	}
	if result.MatchedCount == 0 {
		return ErrLeaseExpired
	}
	return nil
}

func (m *mongoLeaseManager) Renew(ctx context.Context, lease *Lease) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var lastErr error
	for attempt := 0; attempt < m.retryAttempts; attempt++ {
		err := m.renewOnce(ctx, lease)
		if err == nil {
			return nil
		}
		lastErr = err
		if errors.Is(err, ErrLeaseExpired) {
			return err
		}
		if !isTransientNetErr(err) {
			return err
		}
		if attempt < m.retryAttempts-1 {
			sleepBackoff(ctx, attempt)
		}
	}
	return lastErr
}

func (m *mongoLeaseManager) renewOnce(ctx context.Context, lease *Lease) error {
	filter := bson.M{
		"key":        lease.Key,
		"owner":      lease.Owner,
		"generation": lease.Generation,
	}
	update := bson.M{
		"$set": bson.M{
			"updated_at": time.Now(),
		},
	}

	result, err := m.col.UpdateOne(ctx, filter, update)
	if err != nil {
		return err
	}
	if result.MatchedCount == 0 {
		return ErrLeaseExpired
	}
	return nil
}

// isTransientNetErr 判断是否为瞬态网络错误（可重试）。
// 在 retry.go 通用实现基础上，额外检查 MongoDB 特有的网络/超时错误。
func isTransientNetErr(err error) bool {
	if isTransientNetErrBase(err) {
		return true
	}
	if mongo.IsNetworkError(err) {
		return true
	}
	if mongo.IsTimeout(err) {
		return true
	}
	return false
}
