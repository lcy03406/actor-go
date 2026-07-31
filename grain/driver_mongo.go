package grain

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// MongoDriver 基于 MongoDB 的持久化驱动。
// 每个 ActorType 对应一个集合，默认集合名 = actorType，
// 可通过 RegisterCollection 自定义。
//
// 文档结构：
//
//	{
//	  "_id": "actorId",
//	  "generation": NumberLong(1),
//	  ...快照字段...
//	}
//
// Save 使用 ReplaceOne + Upsert，并通过 generation 条件防止过期写入。
type MongoDriver struct {
	db         *mongo.Database
	client     *mongo.Client
	collection map[string]string // actorType → collectionName
}

// NewMongoDriver 创建 MongoDB 驱动。db 为 mongo.Database 实例。
func NewMongoDriver(db *mongo.Database) *MongoDriver {
	return &MongoDriver{
		db:         db,
		client:     nil,
		collection: make(map[string]string),
	}
}

// NewMongoDriverFromClient 从 mongo.Client 创建驱动，指定数据库名。
func NewMongoDriverFromClient(client *mongo.Client, dbName string) *MongoDriver {
	return &MongoDriver{
		db:         client.Database(dbName),
		client:     client,
		collection: make(map[string]string),
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
	ID         string `bson:"_id"`
	Generation int64  `bson:"generation"`
	Snapshot   any    `bson:"inline"`
}

func (d *MongoDriver) Load(ctx context.Context, actorType string, id string, dst any) error {
	col := d.col(actorType)
	var doc mongoDoc
	doc.Snapshot = dst
	err := col.FindOne(ctx, bson.M{"_id": id}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return ErrNotFound
	}
	return err
}

func (d *MongoDriver) Save(ctx context.Context, actorType string, id string, src any, gen int64) error {
	col := d.col(actorType)

	// 先尝试 replace：仅当 generation <= gen 时写入，防止过期写入
	filter := bson.M{
		"_id":        id,
		"generation": bson.M{"$lte": gen},
	}

	doc := mongoDoc{
		ID:         id,
		Generation: gen,
		Snapshot:   src,
	}

	opts := options.Replace().SetUpsert(true)
	_, err := col.ReplaceOne(ctx, filter, doc, opts)
	if err != nil {
		return err
	}
	return nil
}
