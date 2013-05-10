package redis

import (
	"bufio"
	"errors"
	"io"
	"net"
	"time"
)

// http://redis.io/topics/protocol

var (
	ErrInvalidReplyMarker  = errors.New("redis: invalid reply marker")
	ErrInvalidValue        = errors.New("redis: invalid value")
	ErrInvalidArgumentType = errors.New("redis: invalid argument type")
)

type ErrReply string

func (e ErrReply) Error() string {
	return "redis: error response from server: " + string(e)
}

const (
	statusReplyMarker    = '+' // e.g. "+OK\r\n"
	errorReplyMarker     = '-' // e.g. "-ERR unknown command\r\n"
	integerReplyMarker   = ':' // e.g. ":1234\r\n"
	bulkReplyMarker      = '$' // e.g. "$4\r\n1234\r\n" or "$-1\r\n" for NULL
	multiBulkReplyMarker = '*' // e.g. "*2\r\n<other reply><other reply>" or "*-1" for NULL
	eol                  = "\r\n"

	connectionBufferSize = 1024
)

type redisConnection struct {
	nc      net.Conn
	rw      *bufio.ReadWriter
	timeout time.Duration
	buf     []byte
}

func (rc *redisConnection) flush() error {
	return rc.rw.Flush()
}

func (rc *redisConnection) close() error {
	return rc.nc.Close()
}

func (rc *redisConnection) writeI64(marker uint8, i int64) error {
	if err := rc.rw.WriteByte(marker); err != nil {
		return err
	}
	if _, err := rc.rw.Write(itob64(i, rc.buf)); err != nil {
		return err
	}
	_, err := rc.rw.WriteString(eol)
	return err
}

func (rc *redisConnection) writeInteger(v int64) error {
	return rc.writeI64(integerReplyMarker, v)
}

func (rc *redisConnection) writeArgumentCount(count int) error {
	return rc.writeI64(multiBulkReplyMarker, int64(count))
}

func (rc *redisConnection) writeBulkString(v string) error {
	if err := rc.writeI64(bulkReplyMarker, int64(len(v))); err != nil {
		return err
	}
	if _, err := rc.rw.WriteString(v); err != nil {
		return err
	}
	_, err := rc.rw.WriteString(eol)
	return err
}

func (rc *redisConnection) writeBulkBytes(v []byte) error {
	if err := rc.writeI64(bulkReplyMarker, int64(len(v))); err != nil {
		return err
	}
	if _, err := rc.rw.Write(v); err != nil {
		return err
	}
	_, err := rc.rw.WriteString(eol)
	return err
}

func (rc *redisConnection) writeStatus(status string) error {
	if err := rc.rw.WriteByte(statusReplyMarker); err != nil {
		return err
	}
	if _, err := rc.rw.WriteString(status); err != nil {
		return err
	}
	_, err := rc.rw.WriteString(eol)
	return err
}

func (rc *redisConnection) readI64() (int64, byte, error) {
	marker, err := rc.rw.ReadByte()
	if err != nil {
		return -1, marker, err
	} else if marker == errorReplyMarker {
		line, isPrefix, err := rc.rw.ReadLine()
		if err != nil {
			return -1, marker, err
		}
		// Errors should never be larger than the buffer
		if isPrefix {
			return -1, marker, ErrInvalidValue
		}
		return -1, marker, ErrReply(string(line))
	}
	line, isPrefix, err := rc.rw.ReadLine()
	if err != nil {
		return -1, marker, err
	}
	// The line should never be too long when reading an integer
	if isPrefix {
		return -1, marker, ErrInvalidValue
	}
	n, err := btoi64(line)
	return n, marker, err
}

func (rc *redisConnection) readInteger() (int64, error) {
	i, marker, err := rc.readI64()
	if err == nil && marker != integerReplyMarker {
		err = ErrInvalidReplyMarker
	}
	return i, err
}

func (rc *redisConnection) readArgumentCount() (int, error) {
	i, marker, err := rc.readI64()
	if err == nil && marker != multiBulkReplyMarker {
		err = ErrInvalidReplyMarker
	}
	return int(i), err
}

func (rc *redisConnection) readBulkString() (string, error) {
	n, marker, err := rc.readI64()
	if err != nil {
		return "", err
	}
	if n < 0 {
		return "", nil
	}
	if marker != bulkReplyMarker {
		return "", ErrInvalidReplyMarker
	}
	// +2 for \r\n
	b := rc.buf
	if n+2 > int64(len(rc.buf)) {
		b = make([]byte, n+2)
	} else {
		b = b[:n+2]
	}
	if _, err := io.ReadFull(rc.rw, b); err != nil {
		return "", err
	}
	return string(b[:n]), nil
}

func (rc *redisConnection) readBulkBytes() ([]byte, error) {
	n, marker, err := rc.readI64()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, nil
	}
	if marker != bulkReplyMarker {
		return nil, ErrInvalidReplyMarker
	}
	// +2 for \r\n
	b := make([]byte, n+2)
	if _, err := io.ReadFull(rc.rw, b); err != nil {
		return nil, err
	}
	return b[:n], nil
}

func (rc *redisConnection) readStatus() (string, error) {
	m, err := rc.rw.ReadByte()
	if err != nil {
		return "", err
	}
	line, isPrefix, err := rc.rw.ReadLine()
	if err != nil {
		return "", err
	}
	// Status/error should never be larger than the buffer
	if isPrefix {
		return "", ErrInvalidValue
	}
	switch m {
	case statusReplyMarker:
		return string(line), nil
	case errorReplyMarker:
		return "", ErrReply(string(line))
	case bulkReplyMarker:
		if len(line) == 2 && line[0] == '-' && line[1] == '1' {
			return "", nil
		}
	}
	return "", ErrInvalidReplyMarker
}

//

func (rc *redisConnection) sendCommand(cmd string, args ...interface{}) error {
	if err := rc.writeArgumentCount(1 + len(args)); err != nil {
		return err
	}
	if err := rc.writeBulkString(cmd); err != nil {
		return err
	}
	for _, a := range args {
		var err error
		switch v := a.(type) {
		case int:
			err = rc.writeBulkBytes(itob64(int64(v), rc.buf))
		case int64:
			err = rc.writeBulkBytes(itob64(v, rc.buf))
		case []byte:
			err = rc.writeBulkBytes(v)
		case string:
			err = rc.writeBulkString(v)
		default:
			return ErrInvalidArgumentType
		}
		if err != nil {
			return err
		}
	}
	return nil
}
