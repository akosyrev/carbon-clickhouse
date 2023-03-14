package uploader

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
)

// Dumps rowbinary to file
func NewIndexDump(base *Base) *Index {
	u := &Index{}
	u.cached = newCached(base)
	u.cached.parser = u.parseFile
	u.Base.handler = u.fupload
	return u
}

func (u *Index) fupload(ctx context.Context, logger *zap.Logger, filename string) (uint64, error) {
	fmt.Printf("Fupload called for %s\n", filename)
	var n uint64
	var err error
	var newSeries map[string]bool

	pipeReader, pipeWriter := io.Pipe()
	writer := bufio.NewWriter(pipeWriter)
	startTime := time.Now()

	uploadResult := make(chan error, 1)

	u.Go(func(ctx context.Context) {
		err = u.finsertRowBinary(
			u.query,
			pipeReader,
			filename,
		)
		uploadResult <- err
		if err != nil {
			pipeReader.CloseWithError(err)
		}
	})

	n, newSeries, err = u.parser(filename, writer)
	if err == nil {
		err = writer.Flush()
	}
	pipeWriter.CloseWithError(err)

	var uploadErr error

	select {
	case uploadErr = <-uploadResult:
		// pass
	case <-ctx.Done():
		return n, fmt.Errorf("upload aborted")
	}

	if err != nil {
		return n, err
	}

	if uploadErr != nil {
		return n, uploadErr
	}

	// commit new series
	u.existsCache.Merge(newSeries, startTime.Unix())

	fmt.Printf("Fupload will return %d\n", n)

	return n, nil
}

func (u *Base) finsertRowBinary(table string, data io.Reader, filename string) error {
	fmt.Printf("Finsert called for %s\n", filename)
	if u.config.CompressData {
		data = compress(data)
	}

	d, fn := filepath.Split(filename)
	f, err := os.OpenFile(filepath.Join(d, "rb_"+fn), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := &bytes.Buffer{}
	buf.ReadFrom(data)
	rb_data := buf.Bytes()
	if _, err = f.Write(rb_data); err != nil {
		return err
	}
	fmt.Printf("Finsert writed %d bytes\n", len(rb_data))

	return nil
}
