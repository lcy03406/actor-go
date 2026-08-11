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
//	  "snapshot": { ...用户快照字段... }
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
	Snapshot   any       `bson:"snapshot"`
}

// Load 加载快照并获取租约，原子操作（upsert 语义，文档不存在时自动创建）。
//   - owner 为空、租约已过期、或已被本节点持有 → 抢占/续租，generation+1，返回数据 + 新 LeaseInfo
//     （首次激活时文档不存在，upsert 创建文档，generation 置 1）
//   - 被其他节点持有且未过期 → 返回 ErrLeaseTaken（含持有者信息）
func (d *MongoDriver) Load(ctx context.Context, actorType string, id string, owner string, dst any) (*LeaseInfo, error) {
	col := d.col(actorType)
	now := time.Now()
	expireTime := now.Add(-d.leaseTimeout)

	// 原子抢占/续租：仅当无主、已过期、或已被本节点持有时匹配。
	// 注意 upsert=false：避免"被其他节点持有且未过期"时误插入导致 _id 重复键冲突。
	filter := bson.M{
		"_id": id,
		"$or": []bson.M{
			{"owner": ""},
			{"owner": owner},
			{"updated_at": bson.M{"$lt": expireTime}},
		},
	}
	// 使用聚合管道更新，确保与 Redis/JSON driver 语义一致：
	// 仅当发生"抢占/首次"（owner 由空变我、由他人变我、或租约过期）时 generation +1；
	// 同一 owner 幂等续租时 generation 保持不变。
	update := mongo.Pipeline{
		{bson.E{Key: "$set", Value: bson.M{
			"owner":      owner,
			"updated_at": now,
			"generation": bson.M{"$cond": bson.M{
				"if": bson.M{"$or": []any{
					bson.M{"owner": ""},
					bson.M{"$ne": []any{"$owner", owner}},
					bson.M{"$lt": []any{"$updated_at", expireTime}},
				}},
				"then": bson.M{"$add": []any{bson.M{"$ifNull": []any{"$generation", 0}}, 1}},
				"else": bson.M{"$ifNull": []any{"$generation", 1}},
			}},
		}}},
	}

	opts := options.FindOneAndUpdate().
		SetReturnDocument(options.After)

	var doc mongoDoc
	doc.Snapshot = dst
	err := col.FindOneAndUpdate(ctx, filter, update, opts).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		// 文档不存在 或 被其他节点持有且未过期。读取以区分。
		var existing mongoDoc
		findErr := col.FindOne(ctx, bson.M{"_id": id}).Decode(&existing)
		if findErr == mongo.ErrNoDocuments {
			// 不存在 → 首次激活，upsert 创建文档（generation 置 1）。
			createUpdate := bson.M{
				"$set": bson.M{
					"owner":      owner,
					"updated_at": now,
					"generation": 1,
				},
				"$setOnInsert": bson.M{
					"_id":     id,
					"snapshot": dst,
				},
			}
			createOpts := options.FindOneAndUpdate().
				SetReturnDocument(options.After).
				SetUpsert(true)
			var created mongoDoc
			if cErr := col.FindOneAndUpdate(ctx, bson.M{"_id": id}, createUpdate, createOpts).Decode(&created); cErr != nil {
				return nil, cErr
			}
			// 首次激活：返回 ErrNotFound，但租约已获取。
			return &LeaseInfo{Key: id, Owner: created.Owner, Generation: created.Generation}, ErrNotFound
		}
		if findErr != nil {
			return nil, findErr
		}
		// 文档存在但被其他节点持有且未过期 → 返回 ErrLeaseTaken。
		return nil, &ErrLeaseTaken{
			Key:        id,
			Owner:      existing.Owner,
			Generation: existing.Generation,
		}
	}
	if err != nil {
		return nil, err
	}

	// 更新后 owner 不匹配 → 租约被他人持有（理论不会发生，因 filter 已限定 owner）
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
// src 为 nil 时仅续租（更新 generation 与 updated_at），不覆盖 inline 快照字段，
// 用于 snapshot 返回 nil 时"本次不存盘但仍保活租约"。
func (d *MongoDriver) Save(ctx context.Context, actorType string, id string, owner string, src any, gen int64) error {
	col := d.col(actorType)
	now := time.Now()

	filter := bson.M{
		"_id":        id,
		"owner":      owner,
		"generation": gen,
	}

	if src == nil {
		update := bson.M{
			"$set": bson.M{
				"generation": gen,
				"updated_at": now,
			},
		}
		result, err := col.UpdateOne(ctx, filter, update)
		if err != nil {
			return err
		}
		if result.MatchedCount == 0 {
			return errors.New("grain: save failed, lease expired or taken by another owner")
		}
		return nil
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

// ForceRelease 强制释放租约，不检查 owner 和 generation。
func (d *MongoDriver) ForceRelease(ctx context.Context, actorType string, id string) (int64, error) {
	col := d.col(actorType)
	now := time.Now()

	filter := bson.M{"_id": id}
	update := bson.M{
		"$set": bson.M{
			"owner":      "",
			"updated_at": now,
		},
		"$inc": bson.M{"generation": 1},
	}

	opts := options.FindOneAndUpdate().
		SetReturnDocument(options.After).
		SetUpsert(true)

	var doc mongoDoc
	err := col.FindOneAndUpdate(ctx, filter, update, opts).Decode(&doc)
	if err != nil {
		return 0, err
	}
	return doc.Generation, nil
}
