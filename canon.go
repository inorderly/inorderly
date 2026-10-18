// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

package inorderly

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"unicode/utf8"
)

// canonicalJSON rewrites a JSON value in one fixed form: no whitespace,
// object keys sorted by their bytes, strings escaped one way, numbers kept
// exactly as written. The same value written by two different encoders
// comes out the same. A key that appears twice in one object is an error.
func canonicalJSON(raw []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	out, err := canonValue(dec, nil)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errors.New("there is more after the value")
	}
	return out, nil
}

func canonValue(dec *json.Decoder, b []byte) ([]byte, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch v := tok.(type) {
	case json.Delim:
		switch v {
		case '{':
			type member struct {
				key string
				val []byte
			}
			var members []member
			seen := map[string]bool{}
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return nil, err
				}
				k := kt.(string)
				if seen[k] {
					return nil, fmt.Errorf("the key %q appears twice in one object", k)
				}
				seen[k] = true
				val, err := canonValue(dec, nil)
				if err != nil {
					return nil, err
				}
				members = append(members, member{k, val})
			}
			if _, err := dec.Token(); err != nil {
				return nil, err
			}
			sort.Slice(members, func(i, j int) bool { return members[i].key < members[j].key })
			b = append(b, '{')
			for i, m := range members {
				if i > 0 {
					b = append(b, ',')
				}
				b = appendString(b, m.key)
				b = append(b, ':')
				b = append(b, m.val...)
			}
			return append(b, '}'), nil
		case '[':
			b = append(b, '[')
			first := true
			for dec.More() {
				if !first {
					b = append(b, ',')
				}
				first = false
				if b, err = canonValue(dec, b); err != nil {
					return nil, err
				}
			}
			if _, err := dec.Token(); err != nil {
				return nil, err
			}
			return append(b, ']'), nil
		}
		return nil, fmt.Errorf("unexpected %v", v)
	case string:
		return appendString(b, v), nil
	case json.Number:
		return append(b, v...), nil
	case bool:
		if v {
			return append(b, "true"...), nil
		}
		return append(b, "false"...), nil
	case nil:
		return append(b, "null"...), nil
	}
	return nil, fmt.Errorf("unexpected token %v", tok)
}

// appendString writes s as a JSON string: quotes, backslashes and control
// characters escaped, everything else as UTF-8, and invalid UTF-8 replaced
// with U+FFFD.
func appendString(b []byte, s string) []byte {
	const hex = "0123456789abcdef"
	b = append(b, '"')
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			switch {
			case c == '"' || c == '\\':
				b = append(b, '\\', c)
			case c == '\n':
				b = append(b, '\\', 'n')
			case c == '\r':
				b = append(b, '\\', 'r')
			case c == '\t':
				b = append(b, '\\', 't')
			case c < 0x20:
				b = append(b, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xf])
			default:
				b = append(b, c)
			}
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			b = append(b, "�"...)
		} else {
			b = append(b, s[i:i+size]...)
		}
		i += size
	}
	return append(b, '"')
}
