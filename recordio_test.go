package recordio_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/tomwei7/recordio"
)

func TestRecordIO(t *testing.T) {
	buf := &bytes.Buffer{}
	writer := recordio.NewWriter(buf)
	for i := 0; i < 10; i++ {
		record := writer.NextRecord()
		fmt.Fprintf(record, "hello record %d", i)
		if err := record.Close(); err != nil {
			t.Error(err)
		}
	}
	readN := 0
	reader := recordio.NewReader(buf)
	for reader.Next() {
		readN++
		data, err := ioutil.ReadAll(reader)
		if err != nil {
			t.Error(err)
		}
		if bytes.Index(data, []byte("hello")) == -1 {
			t.Errorf("unexpected data: %s", data)
		}
	}
}
