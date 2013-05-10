package redis

// Modified versions from strconv to avoid allocations

const (
	digits01 = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
	digits10 = "0000000000111111111122222222223333333333444444444455555555556666666666777777777788888888889999999999"
)

func btoi64(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, ErrInvalidValue
	}

	i := 0
	negative := b[0] == '-'
	if negative {
		i++
		if i >= len(b) {
			return 0, ErrInvalidValue
		}
	}
	i64 := int64(0)
	for ; i < len(b); i++ {
		d := int64(b[i]) - '0'
		if d < 0 || d > 9 {
			return 0, ErrInvalidValue
		}
		i64 = (i64 * 10) + d
	}
	if negative {
		i64 = -i64
	}
	return i64, nil
}

func itob64(i int64, b []byte) []byte {
	if len(b) < 1 {
		return b
	}

	if i == 0 {
		b[0] = '0'
		return b[:1]
	}

	var a [24]byte
	u := uint64(i)
	if i < 0 {
		u = uint64(-i)
	}
	o := len(a)
	for u >= 100 {
		o -= 2
		q := u / 100
		j := uintptr(u - q*100)
		a[o+1] = digits01[j]
		a[o+0] = digits10[j]
		u = q
	}
	if u >= 10 {
		o--
		q := u / 10
		a[o] = digits01[uintptr(u-q*10)]
		u = q
	}
	o--
	a[o] = digits01[u]
	if i < 0 {
		o--
		a[o] = '-'
	}
	n := len(a) - o
	if n > len(b) {
		n = len(b)
	}
	copy(b[:n], a[o:])
	return b[:n]
}
