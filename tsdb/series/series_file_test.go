package series

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/models"
)

func TestAppendSeriesKey(t *testing.T) {
	tests := []struct {
		key string
		buf []byte
	}{
		{key: "cpu,host=host1,region=us-west"},
		{key: "cpu,host=host1,region=us-west", buf: make([]byte, 0, 256)},
	}
	for _, test := range tests {
		t.Run(test.key, func(t *testing.T) {
			name, tags := models.ParseKeyBytes([]byte(test.key))
			buf := AppendSeriesKey(test.buf, name, tags)
			name2, tags2 := ParseSeriesKey(buf)
			t.Log(string(name2), tags2.String())
		})
	}
}

func TestParseSeriesKey(t *testing.T) {
	keys := generateSeriesKeys(3, 1, 1, 1)
	tests := []struct {
		key []byte
	}{
		{keys[0]},
		{keys[1]},
		{keys[2]},
	}
	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			name, tags := ParseSeriesKey(test.key)
			t.Log(string(name), tags.String())
		})
	}
}

func makeKey(s string) []byte {
	n, t := models.ParseKeyBytes([]byte(s))
	return AppendSeriesKey(nil, n, t)
}

func TestCompareSeriesKeys(t *testing.T) {
	type st struct {
		name string
		a, b []byte
		exp  int
	}
	tests := []struct {
		name string
		sub  []st
	}{
		{
			name: "names no tags A=B",
			sub: []st{
				{"len(A)=len(B)", makeKey("aaa"), makeKey("aaa"), 0},
			},
		},
		{
			name: "names no tags A<B",
			sub: []st{
				{"len(A)=len(B)", makeKey("aaa"), makeKey("bbb"), -1},
				{"len(A)>len(B)", makeKey("aaaaa"), makeKey("bbb"), -1},
				{"len(A)<len(B)", makeKey("aaa"), makeKey("bbbbbb"), -1},
			},
		},
		{
			name: "names no tags A>B",
			sub: []st{
				{"len(A)=len(B)", makeKey("ccc"), makeKey("bbb"), 1},
				{"len(A)>len(B)", makeKey("ccccc"), makeKey("bbb"), 1},
				{"len(A)<len(B)", makeKey("ccc"), makeKey("bbbbbb"), 1},
			},
		},

		{
			name: "names tags same A=B",
			sub: []st{
				{"len(A)=len(B)", makeKey("aaa,taaa=vaaa"), makeKey("aaa,taaa=vaaa"), 0},
			},
		},
		{
			name: "names tags same A<B",
			sub: []st{
				{"len(A)=len(B)", makeKey("aaa,taaa=vaaa"), makeKey("bbb,taaa=vaaa"), -1},
				{"len(A)>len(B)", makeKey("aaaaa,taaa=vaaa"), makeKey("bbb,taaa=vaaa"), -1},
				{"len(A)<len(B)", makeKey("aaa,taaa=vaaa"), makeKey("bbbbbb,taaa=vaaa"), -1},
			},
		},
		{
			name: "names tags same A>B",
			sub: []st{
				{"len(A)=len(B)", makeKey("ccc,taaa=vaaa"), makeKey("bbb,taaa=vaaa"), 1},
				{"len(A)>len(B)", makeKey("ccccc,taaa=vaaa"), makeKey("bbb,taaa=vaaa"), 1},
				{"len(A)<len(B)", makeKey("ccc,taaa=vaaa"), makeKey("bbbbbb,taaa=vaaa"), 1},
			},
		},

		{
			name: "names tags A<B",
			sub: []st{
				{"len(A)=len(B)", makeKey("aaa,taaa=vaaa"), makeKey("aaa,tbbb=vaaa"), -1},
				{"len(A)>len(B)", makeKey("aaa,taaaaa=vaaa"), makeKey("aaa,tbbb=vaaa"), -1},
				{"len(A)<len(B)", makeKey("aaa,taaa=vaaa"), makeKey("aaa,tbbbbb=vaaa"), -1},
			},
		},
	}
	for _, t0 := range tests {
		t.Run(t0.name, func(t *testing.T) {
			for _, t1 := range t0.sub {
				t.Run(t1.name, func(t *testing.T) {
					got := CompareSeriesKeys(t1.a, t1.b)
					if !cmp.Equal(got, t1.exp) {
						t.Errorf("unexpected value -got/+got\n%s", cmp.Diff(got, t1.exp))
					}
				})
			}
		})
	}
}

var (
	gi int
)

func generateSeries(n int, ntags ...int) (names [][]byte, tags []models.Tags) {
	tcard := 1
	for _, t := range ntags {
		tcard *= t
	}
	card := n * tcard

	names = make([][]byte, card)
	tags = make([]models.Tags, card)

	m := make(map[string]string)

	nc := 0
	name := []byte(fmt.Sprintf("measurement_%03d", nc))

	tc := make([]int, len(ntags))
	tb := len(ntags) - 1
	for j := 0; j < len(ntags); j++ {
		m[fmt.Sprintf("tag_%03d", tb-j)] = fmt.Sprintf("val_%03d", 0)
	}

	for i := 0; i < card; i++ {
		names[i] = name
		tags[i] = models.NewTags(m)

		var j int
		for ; j < len(tc); j++ {
			tc[j] += 1
			if tc[j] < ntags[j] {
				break
			}
			tc[j] = 0
		}

		if j == len(tc) {
			for k := 0; k < len(ntags); k++ {
				m[fmt.Sprintf("tag_%03d", tb-k)] = fmt.Sprintf("val_%03d", 0)
			}
			nc++
			name = []byte(fmt.Sprintf("measurement_%03d", nc))
		} else {
			for k := j; k >= 0; k-- {
				m[fmt.Sprintf("tag_%03d", tb-k)] = fmt.Sprintf("val_%03d", tc[k])
			}
		}
	}
	return
}

func generateSeriesKeys(n int, ntags ...int) [][]byte {
	names, tags := generateSeries(n, ntags...)
	return GenerateSeriesKeys(names, tags)
}

func BenchmarkParseSeriesKey(b *testing.B) {
	keys := generateSeriesKeys(1, 25, 2, 2)
	tags := make(models.Tags, 3)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for j := 0; j < len(keys); j++ {
			ParseSeriesKeyTags(keys[j], tags)
		}
	}
}

func BenchmarkCompareSeriesKeys(b *testing.B) {
	tests := []struct {
		name  string
		tagsN []int
	}{
		{"last diff", []int{2, 1, 1, 1, 1}},
		{"last diff", []int{2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}},
		{"first diff", []int{1, 1, 1, 1, 2}},
		{"first diff", []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2}},
	}

	for _, test := range tests {
		b.Run(fmt.Sprintf("%s %d tags", test.name, len(test.tagsN)), func(b *testing.B) {
			keys := generateSeriesKeys(1, test.tagsN...)
			keyA, keyB := keys[0], keys[1]
			j := 0
			b.ResetTimer()
			b.SetBytes(int64(len(keyA) + len(keyB)))
			for i := 0; i < b.N; i++ {
				j = CompareSeriesKeys(keyA, keyB)
			}
			gi = j
		})
	}
}

type seriesKeys [][]byte

func (a seriesKeys) Len() int      { return len(a) }
func (a seriesKeys) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a seriesKeys) Less(i, j int) bool {
	return CompareSeriesKeys(a[i], a[j]) == -1
}

func BenchmarkCompareSeriesKeys_Sort(b *testing.B) {
	tests := []struct {
		name  string
		nameN int
		tagsN []int
	}{
		{"last two diff", 1, []int{500, 2, 1, 1, 1}},
		{"last two diff", 1, []int{500, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}},
		{"all diff", 1, []int{5, 10, 2, 7, 9, 3, 4}},
		{"all diff", 1, []int{1000, 100, 10}},
	}

	for _, test := range tests {
		card := test.nameN
		for i := 0; i < len(test.tagsN); i++ {
			card *= test.tagsN[i]
		}

		b.Run(fmt.Sprintf("%s %d series %d tags", test.name, card, len(test.tagsN)), func(b *testing.B) {
			keys := generateSeriesKeys(test.nameN, test.tagsN...)
			rand.Seed(0)
			rand.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
			})
			c := 0
			for _, k := range keys {
				c += len(k)
			}

			b.ResetTimer()
			b.SetBytes(int64(c))
			sk := make(seriesKeys, len(keys))
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				copy(sk, keys)
				b.StartTimer()
				sort.Sort(sk)
			}
		})
	}
}

func BenchmarkGenerateSeriesKeys(b *testing.B) {
	names, tags := generateSeries(1, 25, 10, 4)

	b.SetBytes(int64(SeriesKeysSize(names, tags)))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		GenerateSeriesKeys(names, tags)
	}
}
