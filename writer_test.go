package recordio_test

import (
	"io/ioutil"
	"testing"

	"github.com/tomwei7/recordio"
)

func BenchmarkRecordIOWriter(b *testing.B) {
	w := recordio.NewWriter(ioutil.Discard)
	for i := 0; i < b.N; i++ {
		record := w.NextRecord()
		record.Write([]byte("hello world fakdsjfioewfidfj"))
		if err := record.Close(); err != nil {
			b.Error(err)
		}
	}
}
