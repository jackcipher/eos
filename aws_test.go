package eos

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/gotomicro/ego/core/econf"
	"github.com/stretchr/testify/assert"
)

const (
	S3Guid         = "test123"
	S3Content      = "aaaaaa"
	S3ExpectLength = 6
	S3ExpectHead   = 1

	S3CompressGUID    = "test123-snappy"
	S3CompressContent = "snappy-contentsnappy-contentsnappy-contentsnappy-content"
)

var (
	awsCmp *Component
)

func init() {
	confs := `
[eos]
debug = true
storageType = "s3"
s3HttpTransportMaxConnsPerHost = 100
s3HttpTransportIdleConnTimeout = "90s"
accessKeyID = "%s"
accessKeySecret = "%s"
endpoint = "%s"
bucket = "%s"
s3ForcePathStyle = false 
region = "%s"
ssl = false
shards = ["stuvwxyz0123456789", "abcdefghijklmnopqr"]
`
	confs = fmt.Sprintf(confs, os.Getenv("AK_ID"), os.Getenv("AK_SECRET"), os.Getenv("ENDPOINT"), os.Getenv("BUCKET"), os.Getenv("REGION"))
	if err := econf.LoadFromReader(strings.NewReader(confs), toml.Unmarshal); err != nil {
		panic(err)
	}
	cmp := Load("eos").Build()

	awsCmp = cmp
}

func TestS3_GetBucketName(t *testing.T) {
	ctx := context.TODO()
	bucketNamePrefix := os.Getenv("BUCKET")
	awsCmp = Load("eos").Build()
	bn, err := awsCmp.GetBucketName(ctx, "fasdfsfsfsafsf")
	assert.NoError(t, err)
	assert.Equal(t, bucketNamePrefix+"-abcdefghijklmnopqr", bn)

	bn, err = awsCmp.GetBucketName(ctx, "19999999")
	assert.NoError(t, err)
	assert.Equal(t, bucketNamePrefix+"-stuvwxyz0123456789", bn)

	// awsCmp = Load("eos").Build(WithBucket("test-bucket"), WithShards([]string{"abcdefghi", "jklmnopqrstuvwxyz0123456789"}))
	// bn, err = awsCmp.GetBucketName(ctx, "fdsafaddafa")
	// assert.NoError(t, err)
	// assert.Equal(t, "test-bucket-abcdefghi", bn)
	// bn, err = awsCmp.GetBucketName(ctx, "fdsafaddafa1")
	// assert.NoError(t, err)
	// assert.Equal(t, "test-bucket-jklmnopqrstuvwxyz0123456789", bn)
}

func TestS3_Put(t *testing.T) {
	ctx := context.TODO()
	meta := make(map[string]string)
	meta["head"] = strconv.Itoa(S3ExpectHead)
	meta["length"] = strconv.Itoa(S3ExpectLength)

	err := awsCmp.Put(ctx, S3Guid, strings.NewReader(S3Content), meta)
	if err != nil {
		t.Log("aws put error", err)
		t.Fail()
	}

	err = awsCmp.Put(ctx, S3Guid, bytes.NewReader([]byte(S3Content)), meta)
	if err != nil {
		t.Log("aws put error", err)
		t.Fail()
	}
}

func TestS3_CompressAndPut(t *testing.T) {
	ctx := context.TODO()
	meta := make(map[string]string)
	meta["head"] = strconv.Itoa(S3ExpectHead)
	meta["length"] = strconv.Itoa(S3ExpectLength)

	err := awsCmp.PutAndCompress(ctx, S3CompressGUID, strings.NewReader(S3CompressContent), meta)
	if err != nil {
		t.Log("aws put error", err)
		t.Fail()
	}

	err = awsCmp.PutAndCompress(ctx, S3CompressGUID, bytes.NewReader([]byte(S3CompressContent)), meta)
	if err != nil {
		t.Log("aws put error", err)
		t.Fail()
	}
}

func TestS3_Head(t *testing.T) {
	ctx := context.TODO()
	attributes := make([]string, 0)
	attributes = append(attributes, "head", "Content-Length")
	var res map[string]string
	var err error
	var head int
	var length int

	res, err = awsCmp.Head(ctx, S3Guid, attributes)
	if err != nil {
		t.Log("aws head error", err)
		t.Fail()
	}

	head, err = strconv.Atoi(res["head"])
	if err != nil || head != S3ExpectHead {
		t.Log("aws get head fail, res:", res, "err:", err)
		t.Fail()
	}

	attributes = append(attributes, "length")
	res, err = awsCmp.Head(ctx, S3Guid, attributes)
	if err != nil {
		t.Log("aws head error", err)
		t.Fail()
	}

	head, err = strconv.Atoi(res["head"])
	length, err = strconv.Atoi(res["length"])
	contentLength, err := strconv.Atoi(res["Content-Length"])
	if err != nil || head != S3ExpectHead || length != S3ExpectLength || contentLength != S3ExpectLength {
		t.Log("aws get head fail, res:", res, "err:", err)
		t.Fail()
	}
}

func TestS3_Get(t *testing.T) {
	ctx := context.TODO()
	res, err := awsCmp.Get(ctx, S3Guid)
	if err != nil || res != S3Content {
		t.Log("aws get S3Content fail, res:", res, "err:", err)
		t.Fail()
	}

	res1, err := awsCmp.GetAsReader(ctx, S3Guid)
	if err != nil {
		t.Fatal("aws get content as reader fail, err:", err)
	}
	defer res1.Close()

	byteRes, _ := ioutil.ReadAll(res1)
	if string(byteRes) != S3Content {
		t.Fatal("aws get as reader, readAll error")
	}
}

func TestS3_GetWithMeta(t *testing.T) {
	ctx := context.TODO()
	attributes := make([]string, 0)
	attributes = append(attributes, "head")
	res, meta, err := awsCmp.GetWithMeta(ctx, S3Guid, attributes)
	if err != nil {
		t.Fatal("aws get content as reader fail, err:", err)
	}
	defer res.Close()
	byteRes, _ := ioutil.ReadAll(res)
	if string(byteRes) != S3Content {
		t.Fatal("aws get as reader, readAll error")
	}

	head, err := strconv.Atoi(meta["head"])
	if err != nil || head != S3ExpectHead {
		t.Log("aws get head fail, res:", res, "err:", err)
		t.Fail()
	}
}

// compressed content
func TestS3_GetAndDecompress(t *testing.T) {
	ctx := context.TODO()
	res, err := awsCmp.GetAndDecompress(ctx, S3CompressGUID)
	if err != nil || res != S3CompressContent {
		t.Log("aws get S3 conpressed Content fail, res:", res, "err:", err)
		t.Fail()
	}

	res1, err := awsCmp.GetAndDecompressAsReader(ctx, S3CompressGUID)
	if err != nil {
		t.Fatal("aws get compressed content as reader fail, err:", err)
	}

	byteRes, error := ioutil.ReadAll(res1)
	if string(byteRes) != S3CompressContent || error != nil {
		t.Fatal("aws get as reader, readAll error0", string(byteRes), error)
	}
}

// non-compressed content
func TestS3_GetAndDecompress2(t *testing.T) {
	ctx := context.TODO()
	res, err := awsCmp.GetAndDecompress(ctx, S3Guid)
	if err != nil || res != S3Content {
		t.Log("aws get S3Content fail, res:", res, "err:", err)
		t.Fail()
	}

	res1, err := awsCmp.GetAndDecompressAsReader(ctx, S3Guid)
	if err != nil {
		t.Fatal("aws get content as reader fail, err:", err)
	}

	byteRes, _ := ioutil.ReadAll(res1)
	if string(byteRes) != S3Content {
		t.Fatal("aws get as reader, readAll error")
	}
}

func TestS3_SignURL(t *testing.T) {
	ctx := context.TODO()
	res, err := awsCmp.SignURL(ctx, S3Guid, 60)
	if err != nil {
		t.Log("oss signUrl fail, res:", res, "err:", err)
		t.Fail()
	}
}

func TestS3_ListObject(t *testing.T) {
	ctx := context.TODO()
	res, err := awsCmp.ListObject(ctx, S3Guid, S3Guid[0:4], "", 10, "")
	if err != nil || len(res) == 0 {
		t.Log("aws list objects fail, res:", res, "err:", err)
		t.Fail()
	}
}

func TestS3_Del(t *testing.T) {
	ctx := context.TODO()
	err := awsCmp.Del(ctx, S3Guid)
	if err != nil {
		t.Log("aws del key fail, err:", err)
		t.Fail()
	}
}

func TestS3_GetNotExist(t *testing.T) {
	ctx := context.TODO()
	res1, err := awsCmp.Get(ctx, S3Guid+"123")
	if res1 != "" || err != nil {
		t.Log("aws get not exist key fail, res:", res1, "err:", err)
		t.Fail()
	}

	attributes := make([]string, 0)
	attributes = append(attributes, "head")
	res2, err := awsCmp.Head(ctx, S3Guid+"123", attributes)
	if res2 != nil || err != nil {
		t.Log("aws head not exist key fail, res:", res2, "err:", err, err.Error())
		t.Fail()
	}
}

func TestS3_DelMulti(t *testing.T) {
	ctx := context.TODO()
	keys := []string{"aaa", "bbb", "ccc"}
	for _, key := range keys {
		awsCmp.Put(ctx, key, strings.NewReader("2333333"), nil)
	}

	err := awsCmp.DelMulti(ctx, keys)
	if err != nil {
		t.Log("aws del multi keys fail, err:", err)
		t.Fail()
	}

	for _, key := range keys {
		res, err := awsCmp.Get(ctx, key)
		if res != "" || err != nil {
			t.Logf("key:%s should not be exist", key)
			t.Fail()
		}
	}
}

func TestS3_Range(t *testing.T) {
	ctx := context.TODO()
	meta := make(map[string]string)
	err := awsCmp.Put(ctx, guid, strings.NewReader("123456"), meta)
	if err != nil {
		t.Log("aws put error", err)
		t.Fail()
	}

	res, err := awsCmp.Range(ctx, guid, 3, 3)
	if err != nil {
		t.Log("aws range error", err)
		t.Fail()
	}

	byteRes, _ := ioutil.ReadAll(res)
	if string(byteRes) != "456" {
		t.Fatalf("aws range as reader, expect:%s, but is %s", "456", string(byteRes))
	}
}

func TestS3_Exists(t *testing.T) {
	ctx := context.TODO()
	meta := make(map[string]string)
	err := awsCmp.Put(ctx, guid, strings.NewReader("123456"), meta)
	if err != nil {
		t.Log("aws put error", err)
		t.Fail()
	}

	// test exists
	ok, err := awsCmp.Exists(ctx, guid)
	if err != nil {
		t.Log("aws Exists error", err)
		t.Fail()
	}
	if !ok {
		t.Log("aws must Exists, but return not exists")
		t.Fail()
	}

	err = awsCmp.Del(ctx, guid)
	if err != nil {
		t.Log("aws del key fail, err:", err)
		t.Fail()
	}

	// test not exists
	ok, err = awsCmp.Exists(ctx, guid)
	if err != nil {
		t.Log("aws Exists error", err)
		t.Fail()
	}
	if ok {
		t.Log("aws must not Exists, but return exists")
		t.Fail()
	}
}
