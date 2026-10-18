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
	"unicode/utf16"
	"unicode/utf8"
)

// canonicalJSON rewrites a JSON value in one fixed form: no whitespace,
// object keys sorted by their bytes, strings escaped one way, numbers kept
// exactly as written. The same value written by two different encoders
// comes out the same. A key that appears twice in one object is an error.
func canonicalJSON(raw []byte) ([]byte, error) {
	p := canonParser{in: raw}
	p.space()
	out, err := p.value(nil, 0)
	if err != nil {
		return nil, err
	}
	p.space()
	if p.i != len(p.in) {
		return nil, errors.New("there is more after the value")
	}
	return out, nil
}

// canonicalJSONSlow is the same rewrite built on encoding/json. The tests
// check canonicalJSON against it.
func canonicalJSONSlow(raw []byte) ([]byte, error) {
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

// topLevel reads one JSON object and returns each member's value in
// canonical form, by key. A repeated key, anything but an object, or
// anything after it is an error.
func topLevel(raw []byte) (map[string][]byte, error) {
	p := canonParser{in: raw}
	p.space()
	if p.i >= len(p.in) || p.in[p.i] != '{' {
		return nil, p.fail("expected an object")
	}
	members := map[string][]byte{}
	if _, err := p.object(nil, 0, members); err != nil {
		return nil, err
	}
	p.space()
	if p.i != len(p.in) {
		return nil, errors.New("there is more after the event object")
	}
	return members, nil
}

// canonParser reads JSON straight from bytes and writes the canonical form.
type canonParser struct {
	in []byte
	i  int
}

const maxDepth = 10000

func (p *canonParser) space() {
	for p.i < len(p.in) {
		switch p.in[p.i] {
		case ' ', '\t', '\n', '\r':
			p.i++
		default:
			return
		}
	}
}

func (p *canonParser) fail(what string) error {
	return fmt.Errorf("%s at byte %d", what, p.i)
}

func (p *canonParser) value(b []byte, depth int) ([]byte, error) {
	if depth > maxDepth {
		return nil, p.fail("nesting too deep")
	}
	if p.i >= len(p.in) {
		return nil, p.fail("unexpected end")
	}
	switch c := p.in[p.i]; {
	case c == '{':
		return p.object(b, depth, nil)
	case c == '[':
		p.i++
		b = append(b, '[')
		p.space()
		if p.i < len(p.in) && p.in[p.i] == ']' {
			p.i++
			return append(b, ']'), nil
		}
		for {
			var err error
			p.space()
			if b, err = p.value(b, depth+1); err != nil {
				return nil, err
			}
			p.space()
			if p.i >= len(p.in) {
				return nil, p.fail("unexpected end in an array")
			}
			if p.in[p.i] == ',' {
				p.i++
				b = append(b, ',')
				continue
			}
			if p.in[p.i] == ']' {
				p.i++
				return append(b, ']'), nil
			}
			return nil, p.fail("expected , or ]")
		}
	case c == '"':
		s, err := p.str()
		if err != nil {
			return nil, err
		}
		return appendString(b, s), nil
	case c == '-' || (c >= '0' && c <= '9'):
		return p.number(b)
	default:
		for _, lit := range []string{"true", "false", "null"} {
			if bytes.HasPrefix(p.in[p.i:], []byte(lit)) {
				p.i += len(lit)
				return append(b, lit...), nil
			}
		}
		return nil, p.fail("invalid character")
	}
}

// object reads an object; when collect isn't nil, it also gets every
// member's canonical value by key.
func (p *canonParser) object(b []byte, depth int, collect map[string][]byte) ([]byte, error) {
	p.i++ // {
	type member struct {
		key string
		val []byte
	}
	var members []member
	p.space()
	if p.i < len(p.in) && p.in[p.i] == '}' {
		p.i++
		return append(b, '{', '}'), nil
	}
	for {
		p.space()
		if p.i >= len(p.in) || p.in[p.i] != '"' {
			return nil, p.fail("expected a key")
		}
		k, err := p.str()
		if err != nil {
			return nil, err
		}
		p.space()
		if p.i >= len(p.in) || p.in[p.i] != ':' {
			return nil, p.fail("expected :")
		}
		p.i++
		p.space()
		val, err := p.value(nil, depth+1)
		if err != nil {
			return nil, err
		}
		members = append(members, member{k, val})
		p.space()
		if p.i >= len(p.in) {
			return nil, p.fail("unexpected end in an object")
		}
		if p.in[p.i] == ',' {
			p.i++
			continue
		}
		if p.in[p.i] == '}' {
			p.i++
			break
		}
		return nil, p.fail("expected , or }")
	}
	sort.Slice(members, func(i, j int) bool { return members[i].key < members[j].key })
	for i := 1; i < len(members); i++ {
		if members[i].key == members[i-1].key {
			return nil, fmt.Errorf("the key %q appears twice in one object", members[i].key)
		}
	}
	for _, m := range members {
		if collect != nil {
			collect[m.key] = m.val
		}
	}
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
}

func (p *canonParser) number(b []byte) ([]byte, error) {
	start := p.i
	digits := func() int {
		n := 0
		for p.i < len(p.in) && p.in[p.i] >= '0' && p.in[p.i] <= '9' {
			p.i++
			n++
		}
		return n
	}
	if p.in[p.i] == '-' {
		p.i++
	}
	if p.i < len(p.in) && p.in[p.i] == '0' {
		p.i++
	} else if digits() == 0 {
		return nil, p.fail("invalid number")
	}
	if p.i < len(p.in) && p.in[p.i] == '.' {
		p.i++
		if digits() == 0 {
			return nil, p.fail("invalid number")
		}
	}
	if p.i < len(p.in) && (p.in[p.i] == 'e' || p.in[p.i] == 'E') {
		p.i++
		if p.i < len(p.in) && (p.in[p.i] == '+' || p.in[p.i] == '-') {
			p.i++
		}
		if digits() == 0 {
			return nil, p.fail("invalid number")
		}
	}
	return append(b, p.in[start:p.i]...), nil
}

// str reads a JSON string and returns it decoded, the way encoding/json
// decodes it: invalid UTF-8 and lone surrogates become U+FFFD.
func (p *canonParser) str() (string, error) {
	p.i++ // opening quote
	start := p.i
	// Fast path: no escapes and plain ASCII.
	for p.i < len(p.in) {
		c := p.in[p.i]
		if c == '"' {
			s := string(p.in[start:p.i])
			p.i++
			return s, nil
		}
		if c == '\\' || c < 0x20 || c >= utf8.RuneSelf {
			break
		}
		p.i++
	}
	out := append([]byte(nil), p.in[start:p.i]...)
	for p.i < len(p.in) {
		c := p.in[p.i]
		switch {
		case c == '"':
			p.i++
			return string(out), nil
		case c < 0x20:
			return "", p.fail("control character in a string")
		case c == '\\':
			if p.i+1 >= len(p.in) {
				return "", p.fail("unexpected end in a string")
			}
			e := p.in[p.i+1]
			p.i += 2
			switch e {
			case '"', '\\', '/':
				out = append(out, e)
			case 'b':
				out = append(out, '\b')
			case 'f':
				out = append(out, '\f')
			case 'n':
				out = append(out, '\n')
			case 'r':
				out = append(out, '\r')
			case 't':
				out = append(out, '\t')
			case 'u':
				r, ok := p.hex4()
				if !ok {
					return "", p.fail("invalid \\u escape")
				}
				if utf16.IsSurrogate(r) {
					r2 := rune(utf8.RuneError)
					if p.i+1 < len(p.in) && p.in[p.i] == '\\' && p.in[p.i+1] == 'u' {
						save := p.i
						p.i += 2
						lo, ok := p.hex4()
						if !ok {
							return "", p.fail("invalid \\u escape")
						}
						if dec := utf16.DecodeRune(r, lo); dec != utf8.RuneError {
							r2 = dec
						} else {
							p.i = save // a lone surrogate; the next escape stands alone
						}
					}
					r = r2
				}
				out = utf8.AppendRune(out, r)
			default:
				return "", p.fail("invalid escape")
			}
		case c < utf8.RuneSelf:
			out = append(out, c)
			p.i++
		default:
			r, size := utf8.DecodeRune(p.in[p.i:])
			if r == utf8.RuneError && size == 1 {
				out = append(out, "\ufffd"...)
			} else {
				out = append(out, p.in[p.i:p.i+size]...)
			}
			p.i += size
		}
	}
	return "", p.fail("unexpected end in a string")
}

func (p *canonParser) hex4() (rune, bool) {
	if p.i+4 > len(p.in) {
		return 0, false
	}
	var r rune
	for _, c := range p.in[p.i : p.i+4] {
		switch {
		case c >= '0' && c <= '9':
			r = r<<4 | rune(c-'0')
		case c >= 'a' && c <= 'f':
			r = r<<4 | rune(c-'a'+10)
		case c >= 'A' && c <= 'F':
			r = r<<4 | rune(c-'A'+10)
		default:
			return 0, false
		}
	}
	p.i += 4
	return r, true
}
