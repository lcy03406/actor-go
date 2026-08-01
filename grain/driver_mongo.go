package grain

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// MongoDriver 基于 MongoDB 的持久化驱动，内置租约管理。
// 每个 ActorType 对应一个集合，默认集合名 = actorType，
// 可通过 RegisterCollection 自定义。
//
// 文档结构：
//
//	{
//	  "_id": "actorId",
//	  "owner": "node-1",
//	  "generation": NumberLong(1),
//	  "updated_at": ISODate(...),
//	  ...快照字段...
//	}
//
// Load 使用 findOneAndUpdate 原子完成"加载快照 + 获取/抢占租约"。
// Save 使用 findOneAndUpdate 原子完成"保存快照 + 续租"。
type MongoDriver struct {
	db           *mongo.Database
	client       *mongo.Client
	collection   map[string]string // actorType → collectionName
	nodeId       string
	leaseTimeout time.Duration
}

// NewMongoDriver 创建 MongoDB 驱动。db 为 mongo.Database 实例。
// nodeId 是当前节点标识，leaseTimeout 是租约超时时间（默认 DefaultLeaseTimeout）。
func NewMongoDriver(db *mongo.Database, nodeId string, leaseTimeout time.Duration) *MongoDriver {
	if leaseTimeout <= 0 {
		leaseTimeout = DefaultLeaseTimeout
	}
	return &MongoDriver{
		db:           db,
		client:       nil,
		collection:   make(map[string]string),
		nodeId:       nodeId,
		leaseTimeout: leaseTimeout,
	}
}

// NewMongoDriverFromClient 从 mongo.Client 创建驱动，指定数据库名。
func NewMongoDriverFromClient(client *mongo.Client, dbName string, nodeId string, leaseTimeout time.Duration) *MongoDriver {
	if leaseTimeout <= 0 {
		leaseTimeout = DefaultLeaseTimeout
	}
	return &MongoDriver{
		db:           client.Database(dbName),
		client:       client,
		collection:   make(map[string]string),
		nodeId:       nodeId,
		leaseTimeout: leaseTimeout,
	}
}

// RegisterCollection 为 actorType 指定集合名。默认使用 actorType 本身。
func (d *MongoDriver) RegisterCollection(actorType string, collectionName string) {
	d.collection[actorType] = collectionName
}

func (d *MongoDriver) col(actorType string) *mongo.Collection {
	name, ok := d.collection[actorType]
	if !ok {
		name = actorType
	}
	return d.db.Collection(name)
}

// mongoDoc 是 MongoDB 中存储的文档结构。
// 用户快照字段通过 bson inline 展平到文档顶层。
type mongoDoc struct {
	ID         string    `bson:"_id"`
	Owner      string    `bson:"owner"`
	Generation int64     `bson:"generation"`
	UpdatedAt  time.Time `bson:"updated_at"`
	Snapshot   any       `bson:"inline"`
}

// Load 加载快照并获取租约，原子操作。
//   - 文档不存在 → upsert 创建，设置 owner + generation=1，返回 ErrNotFound + LeaseInfo
//   - owner 为空或租约已过期 → 抢占，generation+1，返回数据 + 新 LeaseInfo
//   - 被其他节点持有且未过期 → 返回 ErrLeaseTaken（含持有者信息）
//   - 已被本节点持有 → 幂等返回，续租
func (d *MongoDriver) Load(ctx context.Context, actorType string, id string, owner string, dst any) (*LeaseInfo, error) {
	col := d.col(actorType)
	now := time.Now()
	expireTime := now.Add(-d.leaseTimeout)

	// 条件：无主、已过期、或已被本节点持有（幂等）
	filter := bson.M{
		"_id": id,
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
		"$setOnInsert": bson.M{
			"_id": id,
		},
	}

	opts := options.FindOneAndUpdate().
		SetReturnDocument(options.After).
		SetUpsert(true)

	var doc mongoDoc
	doc.Snapshot = dst
	err := col.FindOneAndUpdate(ctx, filter, update, opts).Decode(&doc)
	if err != nil {
		return nil, err
	}

	// 更新后 owner 不匹配 → 租约被他人持有
	if doc.Owner != owner {
		return nil, &ErrLeaseTaken{
			Key:        id,
			Owner:      doc.Owner,
			Generation: doc.Generation,
		}
	}

	lease := &LeaseInfo{Key: id, Owner: doc.Owner, Generation: doc.Generation}
	return lease, nil
}

// Save 保存快照并续租。
// 通过 (id, owner, generation) 三元组校验，防止过期写入。
func (d *MongoDriver) Save(ctx context.Context, actorType string, id string, owner string, src any, gen int64) error {
	col := d.col(actorType)
	now := time.Now()

	filter := bson.M{
		"_id":        id,
		"owner":      owner,
		"generation": gen,
	}

	doc := mongoDoc{
		ID:         id,
		Owner:      owner,
		Generation: gen,
		UpdatedAt:  now,
		Snapshot:   src,
	}

	opts := options.Replace().SetUpsert(false)
	result, err := col.ReplaceOne(ctx, filter, doc, opts)
	if err != nil {
		return err
	}
	if result.MatchedCount == 0 {
		return errors.New("grain: save failed, lease expired or taken by another owner")
	}
	return nil
}

// Release 释放租约（清空 owner），不删除数据。
func (d *MongoDriver) Release(ctx context.Context, actorType string, id string, owner string, gen int64) error {
	col := d.col(actorType)
	filter := bson.M{
		"_id":        id,
		"owner":      owner,
		"generation": gen,
	}
	update := bson.M{
		"$set": bson.M{
			"owner":      "",
			"updated_at": time.Now(),
		},
	}
	result, err := col.UpdateOne(ctx, filter, update)
	if err != nil {
		return err
	}
	if result.MatchedCount == 0 {
		return errors.New("grain: release failed, lease expired or taken by another owner")
	}
	return nil
}
