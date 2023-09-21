package eos

/**
Put your environment configuration in ".env-oss"
*/

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
	"github.com/golang/snappy"
	"github.com/gotomicro/ego/core/econf"
	"github.com/stretchr/testify/assert"
)

const (
	guid         = "test123"
	content      = "aaaaaa"
	expectLength = 6
	expectHead   = 1

	compressGUID    = "test123-snappy"
	compressContent = "snappy-contentsnappy-contentsnappy-contentsnappy-content"
)

var (
	ossCmp *Component
)

func init() {
	confs := `
[eos]
debug = false
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
`
	confs = fmt.Sprintf(confs, os.Getenv("AK_ID"), os.Getenv("AK_SECRET"), os.Getenv("ENDPOINT"), os.Getenv("BUCKET"), os.Getenv("REGION"))
	if err := econf.LoadFromReader(strings.NewReader(confs), toml.Unmarshal); err != nil {
		panic(err)
	}
	client := Load("eos").Build()
	ossCmp = client
}

func TestOSS_GetBucketName(t *testing.T) {
	ctx := context.TODO()
	ossCmp = Load("eos").Build(WithBucket("test-bucket"))
	bn, err := ossCmp.GetBucketName(ctx, "fasdfsfsfsafsf")
	assert.NoError(t, err)
	assert.Equal(t, "test-bucket", bn)
	ossCmp = Load("eos").Build(WithBucket("test-bucket"), WithShards([]string{"abcdefghi", "jklmnopqrstuvwxyz0123456789"}))
	bn, err = ossCmp.GetBucketName(ctx, "fdsafaddafa")
	assert.NoError(t, err)
	assert.Equal(t, "test-bucket-abcdefghi", bn)
	bn, err = ossCmp.GetBucketName(ctx, "fdsafaddafa1")
	assert.NoError(t, err)
	assert.Equal(t, "test-bucket-jklmnopqrstuvwxyz0123456789", bn)
}

func TestOSS_Put(t *testing.T) {
	ctx := context.TODO()
	meta := make(map[string]string)
	meta["head"] = strconv.Itoa(expectHead)
	meta["length"] = strconv.Itoa(expectLength)

	err := ossCmp.Put(ctx, guid, strings.NewReader(content), meta)
	if err != nil {
		t.Log("oss put error", err)
		t.Fail()
	}

	err = ossCmp.Put(ctx, guid, bytes.NewReader([]byte(content)), meta)
	if err != nil {
		t.Log("oss put byte array error", err)
		t.Fail()
	}
}

func TestOSS_CompressAndPut(t *testing.T) {
	ctx := context.TODO()
	meta := make(map[string]string)
	meta["head"] = strconv.Itoa(expectHead)
	meta["length"] = strconv.Itoa(expectLength)

	err := ossCmp.PutAndCompress(ctx, compressGUID, strings.NewReader(compressContent), meta)
	if err != nil {
		t.Log("oss put error", err)
		t.Fail()
	}

	err = ossCmp.PutAndCompress(ctx, compressGUID, bytes.NewReader([]byte(compressContent)), meta)
	if err != nil {
		t.Log("oss put error", err)
		t.Fail()
	}
}

func TestOSS_Head(t *testing.T) {
	ctx := context.TODO()
	attributes := make([]string, 0)
	attributes = append(attributes, "head")
	var res map[string]string
	var err error
	var head int
	var length int

	res, err = ossCmp.Head(ctx, guid, attributes)
	if err != nil {
		t.Log("oss head error", err)
		t.Fail()
	}

	head, err = strconv.Atoi(res["head"])
	if err != nil || head != expectHead {
		t.Log("oss get head fail, res:", res, "err:", err)
		t.Fail()
	}

	attributes = append(attributes, "length")
	attributes = append(attributes, "Content-Type")
	res, err = ossCmp.Head(ctx, guid, attributes)
	if err != nil {
		t.Log("oss head error", err)
		t.Fail()
	}

	head, err = strconv.Atoi(res["head"])
	length, err = strconv.Atoi(res["length"])
	if err != nil || head != expectHead || length != expectLength {
		t.Log("oss get head fail, res:", res, "err:", err)
		t.Fail()
	}

	if res["Content-Type"] != "text/plain" {
		t.Log("oss get head Content-Type wrong, res:", res, "err:", err)
		t.Fail()
	}
}

func TestOSS_Get(t *testing.T) {
	ctx := context.TODO()
	res, err := ossCmp.Get(ctx, guid)
	if err != nil || res != content {
		t.Log("oss get content fail, res:", res, "err:", err)
		t.Fail()
	}

	res1, err := ossCmp.GetAsReader(ctx, guid)
	if err != nil {
		t.Fatal("oss get content as reader fail, err:", err)
	}
	defer res1.Close()
	byteRes, _ := ioutil.ReadAll(res1)
	if string(byteRes) != content {
		t.Fatal("oss get as reader, readAll error")
	}

	resBytes, err := ossCmp.GetBytes(ctx, guid, EnableCRCValidation())
	if err != nil || string(resBytes) != content {
		t.Log("oss get content fail, res:", string(resBytes), "err:", err)
		t.Fail()
	}

	res, err = ossCmp.Get(ctx, guid, EnableCRCValidation())
	if err != nil || res != content {
		t.Log("oss get content fail, res:", res, "err:", err)
		t.Fail()
	}
}

func TestOSS_GetWithMeta(t *testing.T) {
	ctx := context.TODO()
	attributes := make([]string, 0)
	attributes = append(attributes, "head")
	res, meta, err := ossCmp.GetWithMeta(ctx, guid, attributes)
	if err != nil {
		t.Fatal("oss get content as reader fail, err:", err)
	}
	defer res.Close()
	byteRes, _ := ioutil.ReadAll(res)
	if string(byteRes) != content {
		t.Fatal("oss get as reader, readAll error")
	}

	head, err := strconv.Atoi(meta["head"])
	if err != nil || head != expectHead {
		t.Log("oss get head fail, res:", res, "err:", err)
		t.Fail()
	}
}

func TestOSS_GetAndDecompress(t *testing.T) {
	ctx := context.TODO()
	reader, meta, err := ossCmp.GetWithMeta(ctx, compressGUID, []string{MetaCompressor})
	if err != nil {
		t.Log("oss get error", err)
		t.Fail()
	}
	assert.Equal(t, "snappy", meta[MetaCompressor])

	rawBytes, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Log("oss read body error", err)
		t.Fail()
	}

	decodedBytes, err := snappy.Decode(nil, rawBytes)
	if err != nil || string(decodedBytes) != compressContent {
		t.Log("snappy decode error", err)
		t.Fail()
	}

	res, err := ossCmp.GetAndDecompress(ctx, compressGUID)
	if err != nil || res != compressContent {
		t.Log("aws get oss content fail, res:", res, "err:", err)
		t.Fail()
	}

	res1, err := ossCmp.GetAndDecompressAsReader(ctx, compressGUID)
	if err != nil {
		t.Fatal("oss get content as reader fail, err:", err)
	}

	byteRes, _ := ioutil.ReadAll(res1)
	if string(byteRes) != compressContent {
		t.Fatal("oss get as reader, readAll error")
	}
}

func TestOSS_GetAndDecompress2(t *testing.T) {
	ctx := context.TODO()
	_, meta, err := ossCmp.GetWithMeta(ctx, guid, []string{MetaCompressor})
	if err != nil {
		t.Log("oss get error", err)
		t.Fail()
	}
	assert.Empty(t, meta[MetaCompressor])

	res, err := ossCmp.GetAndDecompress(ctx, guid)
	if err != nil || res != content {
		t.Log("aws get oss content fail, res:", res, "err:", err)
		t.Fail()
	}

	res1, err := ossCmp.GetAndDecompressAsReader(ctx, guid)
	if err != nil {
		t.Fatal("oss get content as reader fail, err:", err)
	}

	byteRes, _ := ioutil.ReadAll(res1)
	if string(byteRes) != content {
		t.Fatal("oss get as reader, readAll error")
	}
}

func TestOSS_SignURL(t *testing.T) {
	ctx := context.TODO()
	res, err := ossCmp.SignURL(ctx, guid, 60)
	if err != nil {
		t.Log("oss signUrl fail, res:", res, "err:", err)
		t.Fail()
	}
}

func TestOSS_ListObject(t *testing.T) {
	ctx := context.TODO()
	res, err := ossCmp.ListObject(ctx, guid, guid[0:4], "", 10, "")
	if err != nil || len(res) == 0 {
		t.Log("oss list objects fail, res:", res, "err:", err)
		t.Fail()
	}
}

func TestOSS_Del(t *testing.T) {
	ctx := context.TODO()
	err := ossCmp.Del(ctx, guid)
	if err != nil {
		t.Log("oss del key fail, err:", err)
		t.Fail()
	}
}

func TestOSS_DelMulti(t *testing.T) {
	ctx := context.TODO()
	keys := []string{"aaa", "bb0", "ccc"}
	for _, key := range keys {
		ossCmp.Put(ctx, key, strings.NewReader("2333333"), nil)
	}

	err := ossCmp.DelMulti(ctx, keys)
	if err != nil {
		t.Log("aws del multi keys fail, err:", err)
		t.Fail()
	}

	for _, key := range keys {
		res, err := ossCmp.Get(ctx, key)
		if res != "" || err != nil {
			t.Logf("key:%s should not be exist", key)
			t.Fail()
		}
	}
}

func TestOSS_GetNotExist(t *testing.T) {
	ctx := context.TODO()
	res1, err := ossCmp.Get(ctx, guid+"123")
	if res1 != "" || err != nil {
		t.Log("oss get not exist key fail, res:", res1, "err:", err)
		t.Fail()
	}

	attributes := make([]string, 0)
	attributes = append(attributes, "head")
	res2, err := ossCmp.Head(ctx, guid+"123", attributes)
	if res2 != nil || err != nil {
		t.Log("oss head not exist key fail, res:", res2, "err:", err)
		t.Fail()
	}
}

func TestOSS_Range(t *testing.T) {
	ctx := context.TODO()
	meta := make(map[string]string)
	err := ossCmp.Put(ctx, guid, strings.NewReader("123456"), meta)
	if err != nil {
		t.Log("oss put error", err)
		t.Fail()
	}

	res, err := ossCmp.Range(ctx, guid, 3, 3)
	if err != nil {
		t.Log("oss range error", err)
		t.Fail()
	}

	byteRes, _ := ioutil.ReadAll(res)
	if string(byteRes) != "456" {
		t.Fatalf("oss range as reader, expect:%s, but is %s", "456", string(byteRes))
	}
}

func TestOSS_Exists(t *testing.T) {
	ctx := context.TODO()
	meta := make(map[string]string)
	err := ossCmp.Put(ctx, guid, strings.NewReader("123456"), meta)
	if err != nil {
		t.Log("oss put error", err)
		t.Fail()
	}

	// test exists
	ok, err := ossCmp.Exists(ctx, guid)
	if err != nil {
		t.Log("oss Exists error", err)
		t.Fail()
	}
	if !ok {
		t.Log("oss must Exists, but return not exists")
		t.Fail()
	}

	err = ossCmp.Del(ctx, guid)
	if err != nil {
		t.Log("oss del key fail, err:", err)
		t.Fail()
	}

	// test not exists
	ok, err = ossCmp.Exists(ctx, guid)
	if err != nil {
		t.Log("oss Exists error", err)
		t.Fail()
	}
	if ok {
		t.Log("oss must not Exists, but return exists")
		t.Fail()
	}
}
