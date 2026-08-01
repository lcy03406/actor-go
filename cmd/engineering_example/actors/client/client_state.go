// Package client 是连接 Actor：对应真实项目里的 WebSocket session。
//
// 它只做两件事：
//   - 上行：把客户端命令映射成 player 的请求，沿链向下 Call（入口层不含业务规则）；
//   - 下行：收下服务端 Post 过来的报文（真实项目里在这里发出 WebSocket 帧）。
//
// 业务判断一条都不写在这里 —— 连接层越薄，换传输协议时改动就越小。
package client

import (
	"github.com/lcy03406/actor-go/cmd/engineering_example/ids"
	"github.com/lcy03406/actor-go/cmd/engineering_example/protocol"
)

// State 一条连接的状态。连接不持久化：断线即销毁，重连就是一条新连接、新的登录。
type State struct {
	Self  ids.PlayerId   // 本连接操控的玩家（登录后才知道）
	Inbox []protocol.Msg // 收到的下行报文（真实项目里是已发出的帧，示例攒下来便于观察）
}
