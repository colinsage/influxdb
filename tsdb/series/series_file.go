package series

import (
	"bytes"
	"encoding/binary"

	"github.com/influxdata/influxdb/models"
)

// AppendSeriesKey serializes name and tags to a byte slice.
func AppendSeriesKey(dst []byte, name []byte, tags models.Tags) []byte {
	ofssz := SeriesKeyOffsetsSize(len(tags))
	bytsz := SeriesKeyBytesSize(name, tags)

	size := ofssz + bytsz + 4 // + 4 for offset sz and bytes size
	var out []byte
	if dst == nil {
		dst = make([]byte, size)
		out = dst
	} else {
		if cap(dst)-len(dst) < size {
			tmp := make([]byte, len(dst), len(dst)+size)
			copy(tmp, dst)
			dst = tmp
		}
		dst = dst[:len(dst)+size]
		out = dst[len(dst)-size:]
	}

	opos := 0
	bpos := ofssz + 2

	PutUint16(out, opos, uint16(ofssz))
	opos += 2

	PutUint16(out, opos, uint16(len(name)))
	opos += 2

	PutUint16(out, bpos, uint16(bytsz))
	bpos += 2
	copy(out[bpos:bpos+len(name)], name)
	bpos += len(name)

	for i := range tags {
		tag := &tags[i]
		vlen := len(tag.Key)
		PutUint16(out, opos, uint16(vlen))
		copy(out[bpos:bpos+vlen], tag.Key)
		bpos += vlen

		vlen = len(tag.Value)
		PutUint16(out, opos+2, uint16(vlen))
		copy(out[bpos:bpos+vlen], tag.Value)
		bpos += vlen
		opos += 4
	}

	return dst
}

func PutUint16(b []byte, p int, v uint16) {
	binary.BigEndian.PutUint16(b[p:], v)
}

func GetUint16(b []byte, p int) int {
	return int(binary.BigEndian.Uint16(b[p:]))
}

func GetSeriesKeyTagN(data []byte) int {
	ofsN := GetUint16(data, 0)
	return (ofsN - 2) / 4
}

// ParseSeriesKey extracts the name & tags from a series key.
func ParseSeriesKey(data []byte) (name []byte, _ models.Tags) {
	return ParseSeriesKeyTags(data, nil)
}

// ParseSeriesKeyTags extracts the name & tags from a series key, using the tags slice if there is space available.
func ParseSeriesKeyTags(data []byte, tags models.Tags) (name []byte, _ models.Tags) {
	ofsN := GetUint16(data, 0)
	byts := data[ofsN+2+2:]
	bpos := 0

	opos := 2
	vlen := GetUint16(data, opos)
	opos += 2

	name = byts[bpos : bpos+vlen]
	bpos += vlen

	tagN := (ofsN - 2) / 4
	if cap(tags) < tagN {
		tags = make(models.Tags, tagN)
	} else {
		tags = tags[:tagN]
	}

	for i := 0; i < len(tags); i++ {
		tag := &tags[i]

		vlen = GetUint16(data, opos)
		opos += 2
		tag.Key = byts[bpos : bpos+vlen]
		bpos += vlen

		vlen = GetUint16(data, opos)
		opos += 2
		tag.Value = byts[bpos : bpos+vlen]
		bpos += vlen
	}

	return name, tags
}

func CompareSeriesKeys(a, b []byte) int {
	// Handle 'nil' keys.
	if len(a) == 0 && len(b) == 0 {
		return 0
	} else if len(a) == 0 {
		return -1
	} else if len(b) == 0 {
		return 1
	}

	ofsA := GetUint16(a, 0)
	ofsB := GetUint16(b, 0)

	posA := ofsA + 2
	posB := ofsB + 2

	keyLenA := GetUint16(a, posA)
	keyLenB := GetUint16(b, posB)
	posA += 2
	posB += 2

	return bytes.Compare(a[posA:posA+keyLenA], b[posB:posB+keyLenB])
}

// GenerateSeriesKeys generates series keys for a list of names & tags using
// a single large memory block.
func GenerateSeriesKeys(names [][]byte, tagsSlice []models.Tags) [][]byte {
	buf := make([]byte, 0, SeriesKeysSize(names, tagsSlice))
	keys := make([][]byte, len(names))
	for i := range names {
		offset := len(buf)
		buf = AppendSeriesKey(buf, names[i], tagsSlice[i])
		keys[i] = buf[offset:]
	}
	return keys
}

// SeriesKeysSize returns the number of bytes required to encode a list of name/tags.
func SeriesKeysSize(names [][]byte, tagsSlice []models.Tags) int {
	var n int
	for i := range names {
		n += SeriesKeySize(names[i], tagsSlice[i])
	}
	return n
}

func SeriesKeyOffsetsSize(tagsN int) int {
	// all uint16
	return 2 + // name len
		2*2*tagsN // (keyN len + valN len) * tagsN
}

func SeriesKeyBytesSize(name []byte, tags models.Tags) int {
	n := len(name)
	for i := range tags {
		n += len(tags[i].Key)
		n += len(tags[i].Value)
	}
	return n
}

// SeriesKeySize returns the number of bytes required to encode a series key.
func SeriesKeySize(name []byte, tags models.Tags) int {
	return SeriesKeyOffsetsSize(len(tags)) + SeriesKeyBytesSize(name, tags) + 4
}
