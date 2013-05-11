package redis

type Reply interface {
	read(c *redisConnection) error

	Err() error
}

// SimpleReply

type SimpleReply struct {
	err error
}

func (r *SimpleReply) read(c *redisConnection) error {
	_, err := c.readStatusBytes()
	if _, ok := err.(ErrReply); err != nil && !ok {
		return err
	}
	r.err = err
	return nil
}

func (r *SimpleReply) Err() error {
	return r.err
}

// Bulk Reply

type BulkReply struct {
	val []byte
	err error
}

func (r *BulkReply) read(c *redisConnection) error {
	v, err := c.readBulkBytes()
	if _, ok := err.(ErrReply); err != nil && !ok {
		return err
	}
	r.val = v
	r.err = err
	return nil
}

func (r *BulkReply) Value() []byte {
	return r.val
}

func (r *BulkReply) Err() error {
	return r.err
}
