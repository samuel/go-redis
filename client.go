package redis

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	// "strconv"
	"strings"
	"sync"
	"time"
)

const (
	DefaultPort               = 6379
	DefaultMaxIdleConnections = 6
	DefaultTimeout            = time.Millisecond * 100
)

var (
	ErrInvalidStatus = errors.New("redis: invalid status response")
)

var (
	okStatus   = []byte("OK")
	pongStatus = []byte("PONG")
)

type Client struct {
	net, addr string
	timeout   time.Duration

	maxIdleConn  int
	idleConn     []*redisConnection
	idleConnLock sync.Mutex
}

func NewClient(net, addr string) *Client {
	if !strings.Contains(addr, ":") {
		addr = fmt.Sprintf("%s:%d", addr, DefaultPort)
	}
	return &Client{
		net:         net,
		addr:        addr,
		timeout:     DefaultTimeout,
		idleConn:    make([]*redisConnection, 0, DefaultMaxIdleConnections),
		maxIdleConn: DefaultMaxIdleConnections,
	}
}

func (cli *Client) SetMaxIdleConncetions(maxIdle int) {
	cli.maxIdleConn = maxIdle
	// The connection should quickly end up closed as they're used so no
	// need to proactively close them here.
}

func (cli *Client) SetTimeout(timeout time.Duration) {
	cli.timeout = timeout
}

// Status reply

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

//

func (cli *Client) statusRequest(cmd string, args ...interface{}) (status []byte, err error) {
	err = cli.withConnection(func(c *redisConnection) error {
		err := c.sendCommand(cmd, args...)
		if err == nil {
			err = c.flush()
		}
		if err == nil {
			status, err = c.readStatusBytes()
		}
		return err
	})
	return
}

func (cli *Client) integerRequest(cmd string, args ...interface{}) (i int64, err error) {
	err = cli.withConnection(func(c *redisConnection) error {
		err := c.sendCommand(cmd, args...)
		if err == nil {
			err = c.flush()
		}
		if err == nil {
			i, err = c.readInteger()
		}
		return err
	})
	return
}

func (cli *Client) bulkRequest(cmd string, args ...interface{}) (b []byte, err error) {
	err = cli.withConnection(func(c *redisConnection) error {
		err := c.sendCommand(cmd, args...)
		if err == nil {
			err = c.flush()
		}
		if err == nil {
			b, err = c.readBulkBytes()
		}
		return err
	})
	return
}

func (cli *Client) withConnection(fn func(c *redisConnection) error) error {
	c, err := cli.popConnection()
	if err != nil {
		return err
	}

	err = fn(c)
	if _, ok := err.(ErrReply); err != nil && !ok {
		c.nc.Close()
		return err
	}

	cli.pushConnection(c)
	return err
}

func (cli *Client) pushConnection(rc *redisConnection) {
	cli.idleConnLock.Lock()
	defer cli.idleConnLock.Unlock()
	if len(cli.idleConn) >= cli.maxIdleConn {
		rc.close()
	} else {
		cli.idleConn = append(cli.idleConn, rc)
	}
}

func (cli *Client) popConnection() (*redisConnection, error) {
	cli.idleConnLock.Lock()
	defer cli.idleConnLock.Unlock()
	if len(cli.idleConn) == 0 {
		return cli.newConnection()
	}
	rc := cli.idleConn[len(cli.idleConn)-1]
	cli.idleConn = cli.idleConn[:len(cli.idleConn)-1]
	return rc, nil
}

func (cli *Client) newConnection() (*redisConnection, error) {
	// TODO: support connection timeout
	nc, err := net.Dial(cli.net, cli.addr)
	if err != nil {
		return nil, err
	}
	return &redisConnection{
		nc:      nc,
		rw:      bufio.NewReadWriter(bufio.NewReader(nc), bufio.NewWriter(nc)),
		timeout: cli.timeout,
		buf:     make([]byte, connectionBufferSize),
	}, nil
}
