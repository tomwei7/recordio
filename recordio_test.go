package recordio_test

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/tomwei7/recordio"
)

func TestRecordIO(t *testing.T) {
	buf := &bytes.Buffer{}
	w := recordio.NewWriter(buf)
	N := 10
	for i := 0; i < N; i++ {
		rec := w.NewRecord()
		if i%2 == 0 {
			if err := rec.SetCompress(); err != nil {
				t.Error(err)
			}
		}
		_, err := rec.Write([]byte("hello world"))
		if err != nil {
			t.Error(err)
		}
		if err = w.WriteRecord(rec); err != nil {
			t.Error(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Error(err)
	}
	r := recordio.NewReader(buf)
	for r.Next() {
		N--
		data, err := ioutil.ReadAll(r)
		if err != nil {
			t.Error(err)
		}
		if !bytes.Equal(data, []byte("hello world")) {
			t.Errorf("incorrect record: %s", data)
		}
	}
	if N != 0 {
		t.Errorf("expect get 10 record actually get: %d", 10-N)
	}
}

func BenchmarkRecordIO(b *testing.B) {
	r, w, err := os.Pipe()
	if err != nil {
		b.Fatal(err)
	}
	ch := make(chan bool, 1)
	go func() {
		reader := recordio.NewReader(r)
		for reader.Next() {
			_, err := ioutil.ReadAll(reader)
			if err != nil {
				b.Error(err)
			}
		}
		ch <- true
	}()
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i)
	}
	writer := recordio.NewWriter(w)
	for i := 0; i < b.N; i++ {
		rec := writer.NewRecord()
		rec.Write(data)
		writer.WriteRecord(rec)
	}
	writer.Close()
	w.Close()
	<-ch
}

func BenchmarkRecordIOCompress(b *testing.B) {
	r, w, err := os.Pipe()
	if err != nil {
		b.Fatal(err)
	}
	ch := make(chan bool, 1)
	go func() {
		reader := recordio.NewReader(r)
		for reader.Next() {
			_, err := ioutil.ReadAll(reader)
			if err != nil {
				b.Error(err)
			}
		}
		ch <- true
	}()
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i)
	}
	writer := recordio.NewWriter(w)
	for i := 0; i < b.N; i++ {
		rec := writer.NewRecord()
		rec.SetCompress()
		rec.Write(data)
		writer.WriteRecord(rec)
	}
	writer.Close()
	w.Close()
	<-ch
}
