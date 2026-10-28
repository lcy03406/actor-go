package room

import (
	"github.com/lcy03406/actor-go/actor"

	"github.com/lcy03406/actor-go/cmd/engineering_example/ids"
)

// RegisterHandlers 注册房间 actor。
//
// 注册语义就是调度语义，选错不会有编译错误，只会让行为变怪：
//   - RegisterServe：允许"创建"——新房间由第一条 Join 建起来，不需要预先开房；
//   - RegisterQuery：只处理已存在的房间，其余消息打到不存在的房间直接报错，
//     不会凭空建出一个空房间。
func RegisterHandlers(mgr *actor.Manager) {
	actor.Serve(mgr, actor.Options{BufMails: 128}, func(b *actor.RegistryBuilder[ids.RoomId, State]) {
		actor.RegisterServe(b, handleJoin)
		actor.RegisterQuery(b, handleLeave)
		actor.RegisterQuery(b, handleChat)
		actor.RegisterQuery(b, handleBriefChanged)
		actor.RegisterQuery(b, handleReport)
		actor.RegisterQuery(b, handleStatus)
	})
}
