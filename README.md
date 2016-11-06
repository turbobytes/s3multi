# s3multi

[![GoDoc](https://godoc.org/github.com/turbobytes/s3multi?status.svg)](https://godoc.org/github.com/turbobytes/s3multi)

Package s3multi helps with writing key, value pairs into a S3 bucket where all the values for a key gets written into an S3 object of the corresponding key.

I have written similar implementations twice before, and was about to do the same again, but decided to abstract this functionality into a package.

This abstraction makes it feel as if you are streaming data into S3 directly, but thats not the case. The data is written into tmp directory and then bulk PUT'd to S3.

## Usage

	var auth aws.Auth
	auth.AccessKey = "ACCESS_KEY"
	auth.SecretKey = "SECRET_KEY"
	s := s3.New(auth, aws.USEast)
	bucket := s.Bucket("my-bucket-that-exists-in-region") //Where Bucket is https://godoc.org/github.com/AdRoll/goamz/s3#Bucket
	s3w := s3multi.NewS3Writer(bucket, true) //true means we want output to be gzipped
	_, err := s3w.WriteStr("foo.log.gz", "bar\n")
	if err != nil {
		log.Fatal(err)
	}
	_, err = s3w.WriteStr("bar.log.gz", "y\n")
	if err != nil {
		log.Fatal(err)
	}
	_, err = s3w.WriteStr("foo.log.gz", "baz\n")
	if err != nil {
		log.Fatal(err)
	}
	err = s3w.Upload()
	if err != nil {
		log.Fatal(err)
	}

## Design goals

1. Output order is not important
2. Adding newline is users responsibility
3. Overwrites if key exists
4. Allow concurrent operations
5. Return proper errors if anything fails for any reason.

## TODO

1. Make it concurrent
2. Allow configuration -- content type, ACL, meta, etc
3. Benchmark