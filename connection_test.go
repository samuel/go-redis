package redis

import (
	"bufio"
	"bytes"
	"strconv"
	"testing"
)

func TestConnectionAllocations(t *testing.T) {
	b := &bytes.Buffer{}
	b.Grow(1024 * 1024)
	c := &redisConnection{
		nc:  nil,
		rw:  bufio.NewReadWriter(bufio.NewReader(b), bufio.NewWriter(b)),
		buf: make([]byte, 24),
	}

	checkMallocs(t, "writeInteger", 1000, func(t *testing.T) {
		if err := c.writeInteger(123); err != nil {
			t.Fatal(err)
		}
		c.rw.Flush()
	})
	checkMallocs(t, "readInteger", 1000, func(t *testing.T) {
		if v, err := c.readInteger(); err != nil {
			t.Fatal(err)
		} else if v != 123 {
			t.Fatal("readInteger returned wrong value")
		}
	})
	checkMallocs(t, "writeStatus", 1000, func(t *testing.T) {
		if err := c.writeStatus("123"); err != nil {
			t.Fatal(err)
		}
		c.rw.Flush()
	})
	checkMallocs(t, "readStatus", 1000, func(t *testing.T) {
		if v, err := c.readStatus(); err != nil {
			t.Fatal(err)
		} else if v != "123" {
			t.Fatal("readStatus returned wrong value")
		}
	})
	by := []byte{1, 2}
	checkMallocs(t, "writeBulkBytes", 1000, func(t *testing.T) {
		if err := c.writeBulkBytes(by); err != nil {
			t.Fatal(err)
		}
		c.rw.Flush()
	})
	checkMallocs(t, "readBulkBytes", 1000, func(t *testing.T) {
		if v, err := c.readBulkBytes(); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(by, v) {
			t.Fatal("readBulkBytes returned wrong value")
		}
	})
	checkMallocs(t, "writeBulkString", 1000, func(t *testing.T) {
		if err := c.writeBulkString("1234"); err != nil {
			t.Fatal(err)
		}
		c.rw.Flush()
	})
	checkMallocs(t, "readBulkString", 1000, func(t *testing.T) {
		if v, err := c.readBulkString(); err != nil {
			t.Fatal(err)
		} else if v != "1234" {
			t.Fatal("readBulkString returned wrong value")
		}
	})
}

func TestBulkString(t *testing.T) {
	b := &bytes.Buffer{}
	c := &redisConnection{
		nc:  nil,
		rw:  bufio.NewReadWriter(bufio.NewReader(b), bufio.NewWriter(b)),
		buf: make([]byte, 24),
	}
	st := "foo"
	for i := 0; i < 6; i++ {
		if err := c.writeBulkString(st); err != nil {
			t.Fatalf("Error during writeBulkString: %s", err)
		}
	}
	c.rw.Flush()
	for i := 0; i < 6; i++ {
		if o, err := c.readBulkString(); err != nil {
			t.Fatalf("Error during readBulkString: %s", err)
		} else if o != st {
			t.Fatalf("write/readBulkString returned wrong balue %s expected %s", o, st)
		}
	}
}

func TestBulkBytes(t *testing.T) {
	b := &bytes.Buffer{}
	c := &redisConnection{
		nc:  nil,
		rw:  bufio.NewReadWriter(bufio.NewReader(b), bufio.NewWriter(b)),
		buf: make([]byte, 24),
	}
	by := []byte{1, 2, 3, 4}
	if err := c.writeBulkBytes(by); err != nil {
		t.Fatalf("Error during writeBulkBytes: %s", err)
	}
	if err := c.writeBulkBytes(by); err != nil {
		t.Fatalf("Error during writeBulkBytes: %s", err)
	}
	c.rw.Flush()
	if o, err := c.readBulkBytes(); err != nil {
		t.Fatalf("Error during readBulkBytes: %s", err)
	} else if !bytes.Equal(o, by) {
		t.Fatalf("write/readBulkBytes returned wrong balue %+v expected %+v", o, by)
	}
	if o, err := c.readBulkBytes(); err != nil {
		t.Fatalf("Error during readBulkBytes: %s", err)
	} else if !bytes.Equal(o, by) {
		t.Fatalf("write/readBulkBytes returned wrong balue %+v expected %+v", o, by)
	}
}

func TestBtoi64(t *testing.T) {
	if _, err := btoi64([]byte("")); err != ErrInvalidValue {
		t.Fatal("btoi64 should return an error on empty string")
	}
	if _, err := btoi64([]byte("123Q")); err != ErrInvalidValue {
		t.Fatal("btoi64 should return an error on invalid digits")
	}
	if _, err := btoi64([]byte("-")); err != ErrInvalidValue {
		t.Fatal("btoi64 should return an error missing digits for negative number")
	}
	if i, err := btoi64([]byte("123")); err != nil {
		t.Fatalf("btoi64 should not return error for valid positive number: %s", err)
	} else if i != 123 {
		t.Fatalf("btoi64 should return 123 instead of %d for '123'", i)
	}
	if i, err := btoi64([]byte("-123")); err != nil {
		t.Fatalf("btoi64 should not return error for valid negative number: %s", err)
	} else if i != -123 {
		t.Fatalf("btoi64 should return -123 instead of %d for '-123'", i)
	}
}

func TestItob64(t *testing.T) {
	b := make([]byte, 32)
	if o := itob64(1234, b[:2]); !bytes.Equal(o, []byte("12")) {
		t.Fatalf("itob64 failed for short buffer: '%s'", string(o))
	}
	if o := itob64(0, b); !bytes.Equal(o, []byte("0")) {
		t.Fatalf("itob64 failed for 0: '%s'", string(o))
	}
	if o := itob64(1234, b); !bytes.Equal(o, []byte("1234")) {
		t.Fatalf("itob64 failed for 1234: '%s'", string(o))
	}
	if o := itob64(-1234, b); !bytes.Equal(o, []byte("-1234")) {
		t.Fatalf("itob64 failed for -1234: '%s'", string(o))
	}
}

func BenchmarkWriteInteger(b *testing.B) {
	buf := &bytes.Buffer{}
	c := &redisConnection{
		nc: nil,
		rw: bufio.NewReadWriter(bufio.NewReader(buf), bufio.NewWriter(buf)),
	}

	for i := 0; i < b.N; i++ {
		c.writeInteger(1234123412341234)
	}
}

func BenchmarkReadInteger(b *testing.B) {
	buf := &bytes.Buffer{}
	c := &redisConnection{
		nc: nil,
		rw: bufio.NewReadWriter(bufio.NewReader(buf), bufio.NewWriter(buf)),
	}

	for i := 0; i < b.N; i++ {
		c.writeInteger(1234123412341234)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.readInteger()
	}
}

func BenchmarkBtoi64(b *testing.B) {
	by := []byte("123499988877")
	for i := 0; i < b.N; i++ {
		btoi64(by)
	}
}

func BenchmarkParseInt(b *testing.B) {
	s := "123499988877"
	for i := 0; i < b.N; i++ {
		strconv.ParseInt(s, 10, 64)
	}
}

func BenchmarkItob64(b *testing.B) {
	by := make([]byte, 24)
	for i := 0; i < b.N; i++ {
		itob64(-123499988877, by)
	}
}

func BenchmarkFormatInt(b *testing.B) {
	for i := 0; i < b.N; i++ {
		strconv.FormatInt(-123499988877, 10)
	}
}
