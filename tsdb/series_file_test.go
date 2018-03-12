package tsdb_test

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"testing"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
)

// Ensure series file contains the correct set of series.
func TestSeriesFile_Series(t *testing.T) {
	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	series := []Series{
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
		{Name: []byte("mem"), Tags: models.NewTags(map[string]string{"region": "east"})},
	}
	for _, s := range series {
		if _, err := sfile.CreateSeriesListIfNotExists([][]byte{[]byte(s.Name)}, []models.Tags{s.Tags}, nil); err != nil {
			t.Fatal(err)
		}
	}

	// Verify total number of series is correct.
	if n := sfile.SeriesCount(); n != 3 {
		t.Fatalf("unexpected series count: %d", n)
	}

	// Verify all series exist.
	for i, s := range series {
		if seriesID := sfile.SeriesID(s.Name, s.Tags, nil); seriesID == 0 {
			t.Fatalf("series does not exist: i=%d", i)
		}
	}

	// Verify non-existent series doesn't exist.
	if sfile.HasSeries([]byte("foo"), models.NewTags(map[string]string{"region": "north"}), nil) {
		t.Fatal("series should not exist")
	}
}

// Ensure series file can be compacted.
func TestSeriesFileCompactor(t *testing.T) {
	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	// Disable automatic compactions.
	for _, p := range sfile.Partitions() {
		p.CompactThreshold = 0
	}

	var names [][]byte
	var tagsSlice []models.Tags
	for i := 0; i < 10000; i++ {
		names = append(names, []byte(fmt.Sprintf("m%d", i)))
		tagsSlice = append(tagsSlice, models.NewTags(map[string]string{"foo": "bar"}))
	}
	if _, err := sfile.CreateSeriesListIfNotExists(names, tagsSlice, nil); err != nil {
		t.Fatal(err)
	}

	// Verify total number of series is correct.
	if n := sfile.SeriesCount(); n != uint64(len(names)) {
		t.Fatalf("unexpected series count: %d", n)
	}

	// Compact in-place for each partition.
	for _, p := range sfile.Partitions() {
		compactor := tsdb.NewSeriesPartitionCompactor()
		if err := compactor.Compact(p); err != nil {
			t.Fatal(err)
		}
	}

	// Verify all series exist.
	for i := range names {
		if seriesID := sfile.SeriesID(names[i], tagsSlice[i], nil); seriesID == 0 {
			t.Fatalf("series does not exist: %s,%s", names[i], tagsSlice[i].String())
		}
	}
}

// Series represents name/tagset pairs that are used in testing.
type Series struct {
	Name    []byte
	Tags    models.Tags
	Deleted bool
}

// SeriesFile is a test wrapper for tsdb.SeriesFile.
type SeriesFile struct {
	*tsdb.SeriesFile
}

// NewSeriesFile returns a new instance of SeriesFile with a temporary file path.
func NewSeriesFile() *SeriesFile {
	dir, err := ioutil.TempDir("", "tsdb-series-file-")
	if err != nil {
		panic(err)
	}
	return &SeriesFile{SeriesFile: tsdb.NewSeriesFile(dir)}
}

// MustOpenSeriesFile returns a new, open instance of SeriesFile. Panic on error.
func MustOpenSeriesFile() *SeriesFile {
	f := NewSeriesFile()
	f.Logger = logger.New(os.Stdout)
	if err := f.Open(); err != nil {
		panic(err)
	}
	return f
}

// Close closes the log file and removes it from disk.
func (f *SeriesFile) Close() error {
	defer os.RemoveAll(f.Path())
	return f.SeriesFile.Close()
}

var (
	gi int
)

func generateSeries(namesN int, tagsN ...int) (names [][]byte, tags []models.Tags) {
	tcard := 1
	for _, t := range tagsN {
		tcard *= t
	}
	card := namesN * tcard

	names = make([][]byte, card)
	tags = make([]models.Tags, card)

	m := make(map[string]string)

	nc := 0
	name := []byte(fmt.Sprintf("measurement_%03d", nc))

	tc := make([]int, len(tagsN))
	tb := len(tagsN) - 1
	for j := 0; j < len(tagsN); j++ {
		m[fmt.Sprintf("tag_%03d", tb-j)] = fmt.Sprintf("val_%03d", 0)
	}

	for i := 0; i < card; i++ {
		names[i] = name
		tags[i] = models.NewTags(m)

		var j int
		for ; j < len(tc); j++ {
			tc[j] += 1
			if tc[j] < tagsN[j] {
				break
			}
			tc[j] = 0
		}

		if j == len(tc) {
			for k := 0; k < len(tagsN); k++ {
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

func generateSeriesKeys(namesN int, tagsN ...int) [][]byte {
	names, tags := generateSeries(namesN, tagsN...)
	return tsdb.GenerateSeriesKeys(names, tags)
}

func BenchmarkParseSeriesKey(b *testing.B) {
	keys := generateSeriesKeys(1, 25, 2, 2)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for j := 0; j < len(keys); j++ {
			tsdb.ParseSeriesKey(keys[j])
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
				j = tsdb.CompareSeriesKeys(keyA, keyB)
			}
			gi = j
		})
	}
}

type seriesKeys [][]byte

func (a seriesKeys) Len() int      { return len(a) }
func (a seriesKeys) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a seriesKeys) Less(i, j int) bool {
	return tsdb.CompareSeriesKeys(a[i], a[j]) == -1
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

	b.SetBytes(int64(tsdb.SeriesKeysSize(names, tags)))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tsdb.GenerateSeriesKeys(names, tags)
	}
}
