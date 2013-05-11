package redis

import (
	"bufio"
	"errors"
	"fmt"
	"net"
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

func (cli *Client) Pipeline() (*Pipeline, error) {
	cn, err := cli.popConnection()
	if err != nil {
		return nil, err
	}
	return &Pipeline{
		cli: cli,
		cn:  cn,
	}, nil
}

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
