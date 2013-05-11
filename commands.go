package redis

import (
	"bytes"
	"time"
)

func (cli *Client) BGRewriteAOF() error {
	_, err := cli.statusRequest("BGREWRITEAOF")
	return err
}

func (cli *Client) Decr(key string) (int64, error) {
	return cli.integerRequest("DECR", key)
}

func (cli *Client) Get(key string) ([]byte, error) {
	return cli.bulkRequest("GET", key)
}

func (cli *Client) Incr(key string) (int64, error) {
	return cli.integerRequest("INCR", key)
}

func (cli *Client) MGet(key ...string) ([][]byte, error) {
	var out [][]byte
	err := cli.withConnection(func(c *redisConnection) error {
		if err := c.writeArgumentCount(1 + len(key)); err != nil {
			return err
		}
		if err := c.writeBulkString("MGET"); err != nil {
			return err
		}
		for _, k := range key {
			if err := c.writeBulkString(k); err != nil {
				return err
			}
		}

		err := c.flush()
		if err == nil {
			n, m, e := c.readI64()
			if e != nil {
				err = e
			} else if m != multiBulkReplyMarker {
				err = ErrInvalidReplyMarker
			} else {
				out = make([][]byte, n)
				for i := int64(0); i < n; i++ {
					out[i], err = c.readBulkBytes()
					if err != nil {
						break
					}
				}
			}
		}
		return err
	})
	return out, err
}

func (cli *Client) Ping() error {
	status, err := cli.statusRequest("PING")
	if err == nil && !bytes.Equal(status, pongStatus) {
		err = ErrInvalidStatus
	}
	return err
}

func (cli *Client) Quit() error {
	_, err := cli.statusRequest("QUIT")
	return err
}

func (cli *Client) Select(index int) error {
	status, err := cli.statusRequest("SELECT", index)
	if err == nil && !bytes.Equal(status, okStatus) {
		err = ErrInvalidStatus
	}
	return err
}

func (cli *Client) Set(key string, value []byte, expireTime time.Duration) error {
	_, err := cli.set(key, value, expireTime, false, false)
	return err
}

func (cli *Client) SetNX(key string, value []byte, expireTime time.Duration) (bool, error) {
	return cli.set(key, value, expireTime, true, false)
}

func (cli *Client) SetXX(key string, value []byte, expireTime time.Duration) (bool, error) {
	return cli.set(key, value, expireTime, false, true)
}

func (cli *Client) set(key string, value []byte, expireTime time.Duration, nx, xx bool) (bool, error) {
	args := []interface{}{
		key, value,
	}
	if expireTime > 0 {
		ms := expireTime / 1e6
		if ms == 0 {
			args = append(args, "EX", expireTime/1e9)
		} else {
			args = append(args, "PX", ms)
		}
	}
	if nx {
		args = append(args, "NX")
	} else if xx {
		args = append(args, "XX")
	}
	status, err := cli.statusRequest("SET", args...)
	if err == nil && status != nil && !bytes.Equal(status, okStatus) {
		err = ErrInvalidStatus
	}
	return status != nil, err
}
