package s3multi

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"
	"github.com/AdRoll/goamz/s3/s3test"
)

const ofoo = `foo0
foo1
foo2
foo3
foo4
foo5
foo6
foo7
foo8
foo9
`

const obar = `bar0
bar1
bar2
bar3
bar4
bar5
bar6
bar7
bar8
bar9
`

//helper method to ungzip
func gunzip(data []byte) ([]byte, error) {
	b := bytes.NewBuffer(data)
	rd, err := gzip.NewReader(b)
	if err != nil {
		return nil, err
	}
	defer rd.Close()
	return ioutil.ReadAll(rd)
}

//TestS3WriterGzip with gzip enabled. We should do another test without gzip.
//Perhaps add some tests doing writes concurrently.
func TestS3WriterGzip(t *testing.T) {
	mockserver, err := s3test.NewServer(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mockserver.Quit()
	var auth aws.Auth
	auth.AccessKey = "foo"
	auth.SecretKey = "bar"
	s := s3.New(auth, aws.Region{
		Name:                 "faux-region-1",
		S3Endpoint:           mockserver.URL(),
		S3LocationConstraint: true,
	}) //Use real region like aws.USEast to test on real S3
	bucket := s.Bucket("dnsrum-processed")
	err = bucket.PutBucket("")
	if err != nil {
		t.Fatal(err)
	}
	s3w := NewS3Writer(bucket, true)
	//Write some stuff into different keys... synchronously..
	for i := 0; i < 10; i++ {
		_, err = s3w.WriteStr("foo/foo/log.gz", fmt.Sprintf("foo%d\n", i))
		if err != nil {
			t.Fatal(err)
		}
		_, err = s3w.Write("foo/bar/log.gz", []byte(fmt.Sprintf("bar%d\n", i)))
		if err != nil {
			t.Fatal(err)
		}
	}
	err = s3w.Upload()
	if err != nil {
		t.Fatal(err)
	}
	//Validate the output from S3
	d, err := bucket.Get("foo/foo/log.gz")
	if err != nil {
		t.Fatal(err)
	}
	out, err := gunzip(d)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != ofoo {
		t.Errorf("Expected %s got %s", ofoo, string(out))
	}
	//Again for bar
	d, err = bucket.Get("foo/bar/log.gz")
	if err != nil {
		t.Fatal(err)
	}
	out, err = gunzip(d)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != obar {
		t.Errorf("Expected %s got %s", ofoo, string(out))
	}

	//Now try writing to s3w and make sure it returns err.
	_, err = s3w.WriteStr("foo/foo/log.gz", "whatever")
	if err != ErrClosed {
		t.Errorf("Expected error: %s got %s", ErrClosed, err)
	}
}
