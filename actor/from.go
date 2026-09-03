package actor

type From struct {
	Origin string `json:"origin,omitempty"`  //初始来源
	ReqSeq string `json:"req_seq,omitempty"` //请求序号
}

func (f From) String() string {
	return f.Origin + ":" + f.ReqSeq
}

func MakeFrom[A ActorId](id A, seq string) From {
	reqSeq := actorNameOf(id) + "." + seq
	return From{Origin: reqSeq, ReqSeq: reqSeq}
}
