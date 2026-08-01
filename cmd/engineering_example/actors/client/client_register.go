package client

import (
	"github.com/lcy03406/actor-go/actor"

	"github.com/lcy03406/actor-go/cmd/engineering_example/ids"
)

// RegisterHandlers 注册连接 actor。
//
// Login 用 RegisterServe（允许创建）：连接正是由"第一条登录消息"建立的，
// 不需要先手工开一条连接。其余消息都是已存在的连接才会收到的 Query。
func RegisterHandlers(mgr *actor.Manager) {
	actor.Serve(mgr, actor.Options{BufMails: 256}, func(b *actor.RegistryBuilder[ids.ClientId, State]) {
		actor.RegisterServe(b, handleLogin)

		actor.RegisterQuery(b, handleJoin)
		actor.RegisterQuery(b, handleSay)
		actor.RegisterQuery(b, handleAttack)
		actor.RegisterQuery(b, handleStatus)

		actor.RegisterQuery(b, handlePush)
		actor.RegisterQuery(b, handleDrain)
	})
}
