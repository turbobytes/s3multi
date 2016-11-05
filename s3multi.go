package s3multi

import (
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"

	"launchpad.net/goamz/s3"
)

var (
	//ErrClosed occurs when trying to write to a file thats already uploaded(or uploading)
	ErrClosed = fmt.Errorf("S3Writer has already closed the files, not accepting any more input")
)

//S3Writer helps with writing multiple objects into S3. This abstraction lets the user feel they are writing libe by line into multiple S3 keys.
type S3Writer struct {
	bucket    *s3.Bucket
	dogzip    bool
	accepting bool
	mutex     *sync.Mutex
	err       error
	fmap      map[string]*fileobj
}

//Proxy for the file object because we cant use .Name() method of os.File because the writer might be gzip.Writer
type fileobj struct {
	f    io.WriteCloser
	path string
}

//NewS3Writer creates a new S3Writer. if dogzip == True then the file will be gzipped
func NewS3Writer(bucket *s3.Bucket, dogzip bool) *S3Writer {
	s := &S3Writer{
		bucket:    bucket,
		dogzip:    dogzip,
		accepting: true,
		mutex:     &sync.Mutex{},
		fmap:      make(map[string]*fileobj),
	}
	//Initialize things...
	return s
}

//WriteStr writes a string to a s3 object named key. No newline is added.
func (s *S3Writer) WriteStr(key, value string) (int, error) {
	return s.Write(key, []byte(value))
}

//WriteStr writes a byte slice to a s3 object named key. No newline is added.
func (s *S3Writer) Write(key string, value []byte) (int, error) {
	if !s.accepting {
		return 0, ErrClosed
	}
	//Should we really aquire a global lock here? Maybe only lock when doing map operations since file.Write is atomic...
	s.mutex.Lock()
	defer s.mutex.Unlock()
	fobj, err := s.getfile(key)
	if err != nil {
		return 0, err
	}
	//s.ch <- data{key, value}
	return fobj.f.Write(value)
}

//Upload closes the writer and uploads contents to S3. Any future calls to Write will fail
//Blocks until the operation is complete.
func (s *S3Writer) Upload() error {
	if !s.accepting {
		return ErrClosed
	}
	s.accepting = false
	//Block until other writes are done... Block further writes
	s.mutex.Lock()
	for k, fobj := range s.fmap {
		//Close the file..
		fobj.f.Close()
		//stat the file
		f, err := os.Open(fobj.path)
		if err != nil {
			return err
		}
		stat, err := f.Stat()
		if err != nil {
			return err
		}
		//Upload to S3
		err = s.bucket.PutReader(k, f, stat.Size(), "application/octet-stream", "")
		if err != nil {
			return err
		}
		f.Close()
		//Delete tmp file when done...
		os.Remove(fobj.path)
	}
	return s.err
}

//getfile fetches the file from map, and creates it if missing
func (s *S3Writer) getfile(key string) (*fileobj, error) {
	fobj, ok := s.fmap[key]
	//var err error
	if !ok {
		f, err := ioutil.TempFile("", "s3multi")
		if err != nil {
			return nil, err
		}
		path := f.Name()
		//defer os.Remove(f.Name())
		//If compression is enabled then make this a gzip writer
		if s.dogzip {
			gz := gzip.NewWriter(f)
			fobj = &fileobj{gz, path}
		} else {
			fobj = &fileobj{f, path}
		}
		s.fmap[key] = fobj
	}
	return fobj, nil
}
