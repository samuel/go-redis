package redis

type Pipeline struct {
	cli         *Client
	cn          *redisConnection
	replies     []Reply
	transaction bool
}

func (p *Pipeline) Get(key string) *BulkReply {
	r := &BulkReply{}
	p.replies = append(p.replies, r)
	p.cn.sendCommand("GET", key)
	return r
}

func (p *Pipeline) Flush() ([]Reply, error) {
	p.cn.flush()
	for _, r := range p.replies {
		if err := r.read(p.cn); err != nil {
			p.cn.close()
			return nil, err
		}
	}
	p.cli.pushConnection(p.cn)
	p.cn = nil
	p.cli = nil
	return p.replies, nil
}
