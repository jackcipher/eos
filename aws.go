package eos

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"time"

	"github.com/avast/retry-go"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/snappy"
)

var _ Client = (*S3)(nil)

type S3 struct {
	ShardsBucket map[string]string
	BucketName   string
	client       *s3.S3
	cfg          *BucketConfig
	compressor   Compressor
}

// 返回带prefix的key
func (a *S3) keyWithPrefix(key string) string {
	return a.cfg.Prefix + key
}

func (a *S3) Copy(ctx context.Context, srcKey, dstKey string, options ...CopyOption) error {
	cfg := DefaultCopyOptions()
	for _, opt := range options {
		opt(cfg)
	}
	var copySource = srcKey
	if !cfg.rawSrcKey {
		srcBucket, srcKey, err := a.getBucketAndKey(ctx, srcKey)
		if err != nil {
			return err
		}
		copySource = fmt.Sprintf("/%s/%s", srcBucket, srcKey)
	}
	bucketName, dstKey, err := a.getBucketAndKey(ctx, dstKey)
	if err != nil {
		return err
	}
	input := &s3.CopyObjectInput{
		Bucket:            aws.String(bucketName),
		CopySource:        aws.String(copySource),
		Key:               aws.String(dstKey),
		MetadataDirective: aws.String("COPY"),
	}
	if cfg.metaKeysToCopy != nil || cfg.meta != nil {
		input.SetMetadataDirective("REPLACE")
		input.Metadata = make(map[string]*string)
		cfg.metaKeysToCopy = append(cfg.metaKeysToCopy, "Content-Encoding") // always copy content-encoding
		metadata, err := a.Head(ctx, srcKey, cfg.metaKeysToCopy)
		if err != nil {
			return err
		}
		if len(metadata) > 0 {
			for k, v := range metadata {
				if k == "Content-Encoding" {
					input.ContentEncoding = aws.String(v)
					continue
				}
				input.Metadata[k] = aws.String(v)
			}
		}
		// specify new metadata
		for k, v := range cfg.meta {
			input.Metadata[k] = aws.String(v)
		}
	}
	_, err = a.client.CopyObjectWithContext(ctx, input)
	if err != nil {
		return err
	}
	return nil
}

func (a *S3) GetRawSrcKey(ctx context.Context, key string) (string, error) {
	bucketName, fullKey, err := a.getBucketAndKey(ctx, key)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("/%s/%s", bucketName, fullKey), nil
}

func (a *S3) GetBucketName(ctx context.Context, key string) (string, error) {
	bucketName, _, err := a.getBucketAndKey(ctx, key)
	return bucketName, err
}

func (a *S3) getBucketAndKey(ctx context.Context, key string) (string, string, error) {
	key = a.keyWithPrefix(key)
	if a.ShardsBucket != nil && len(a.ShardsBucket) > 0 {
		keyLength := len(key)
		bucketName := a.ShardsBucket[strings.ToLower(key[keyLength-1:keyLength])]
		if bucketName == "" {
			return "", key, errors.New("shards can't find bucket")
		}

		return bucketName, key, nil
	}

	return a.BucketName, key, nil
}

// GetAsReader don't forget to call the close() method of the io.ReadCloser
func (a *S3) GetAsReader(ctx context.Context, key string, options ...GetOptions) (io.ReadCloser, error) {
	bucketName, key, err := a.getBucketAndKey(ctx, key)
	if err != nil {
		return nil, err
	}

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	setS3Options(ctx, options, input)

	result, err := a.client.GetObjectWithContext(ctx, input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchKey {
				return nil, nil
			}
		}
		return nil, err
	}

	return result.Body, err
}

// GetWithMeta don't forget to call the close() method of the io.ReadCloser
func (a *S3) GetWithMeta(ctx context.Context, key string, attributes []string, options ...GetOptions) (io.ReadCloser, map[string]string, error) {
	bucketName, key, err := a.getBucketAndKey(ctx, key)
	if err != nil {
		return nil, nil, err
	}

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	setS3Options(ctx, options, input)

	result, err := a.client.GetObjectWithContext(ctx, input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchKey {
				return nil, nil, nil
			}
		}
		return nil, nil, err
	}
	return result.Body, getS3Meta(ctx, attributes, mergeHttpStandardHeaders(&HeadGetObjectOutputWrapper{
		getObjectOutput: result,
	})), err
}

func (a *S3) Get(ctx context.Context, key string, options ...GetOptions) (string, error) {
	data, err := a.GetBytes(ctx, key, options...)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (a *S3) GetBytes(ctx context.Context, key string, options ...GetOptions) ([]byte, error) {
	result, err := a.get(ctx, key, options...)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}

	body := result.Body
	defer func() {
		if body != nil {
			body.Close()
		}
	}()

	return ioutil.ReadAll(body)
}

func (a *S3) Range(ctx context.Context, key string, offset int64, length int64) (io.ReadCloser, error) {
	readRange := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	bucketName, key, err := a.getBucketAndKey(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("range getBucketAndKey fail, %w", err)
	}
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Range:  &readRange,
	}
	r, err := a.client.GetObjectWithContext(ctx, input)
	if err != nil {
		return nil, err
	}
	return r.Body, nil
}

func (a *S3) GetAndDecompress(ctx context.Context, key string) (string, error) {
	result, err := a.get(ctx, key)
	if err != nil {
		return "", err
	}
	if result == nil {
		return "", nil
	}

	body := result.Body
	defer func() {
		if body != nil {
			body.Close()
		}
	}()

	compressor := result.Metadata["Compressor"]
	if compressor != nil {
		if *compressor != "snappy" {
			return "", errors.New("GetAndDecompress only supports snappy for now, got " + *compressor)
		}

		rawBytes, err := io.ReadAll(body)
		if err != nil {
			return "", err
		}

		decodedBytes, err := snappy.Decode(nil, rawBytes)
		if err != nil {
			if errors.Is(err, snappy.ErrCorrupt) {
				reader := snappy.NewReader(bytes.NewReader(rawBytes))
				data, err := io.ReadAll(reader)
				if err != nil {
					return "", err
				}

				return string(data), nil
			}
			return "", err
		}

		return string(decodedBytes), nil
	}

	data, err := ioutil.ReadAll(body)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (a *S3) GetAndDecompressAsReader(ctx context.Context, key string) (io.ReadCloser, error) {
	result, err := a.GetAndDecompress(ctx, key)
	if err != nil {
		return nil, err
	}

	return ioutil.NopCloser(strings.NewReader(result)), nil
}

func (a *S3) Put(ctx context.Context, key string, reader io.ReadSeeker, meta map[string]string, options ...PutOptions) error {
	bucketName, key, err := a.getBucketAndKey(ctx, key)
	if err != nil {
		return err
	}
	putOptions := DefaultPutOptions()
	for _, opt := range options {
		opt(putOptions)
	}
	input := &s3.PutObjectInput{
		Body:        reader,
		Bucket:      aws.String(bucketName),
		Key:         aws.String(key),
		Metadata:    aws.StringMap(meta),
		ContentType: aws.String(putOptions.contentType),
	}
	if putOptions.contentEncoding != nil {
		input.ContentEncoding = putOptions.contentEncoding
	}
	if putOptions.contentDisposition != nil {
		input.ContentDisposition = putOptions.contentDisposition
	}
	if putOptions.cacheControl != nil {
		input.CacheControl = putOptions.cacheControl
	}
	if putOptions.expires != nil {
		input.Expires = putOptions.expires
	}
	if a.compressor != nil {
		wrapReader, l, err := WrapReader(input.Body)
		if err != nil {
			return err
		}
		if l > a.cfg.CompressLimit {
			input.Body, _, err = a.compressor.Compress(wrapReader)
			if err != nil {
				return err
			}
			encoding := a.compressor.ContentEncoding()
			input.ContentEncoding = &encoding
		} else {
			input.Body = wrapReader
		}
	}

	err = retry.Do(func() error {
		_, err := a.client.PutObjectWithContext(ctx, input)
		if err != nil && reader != nil {
			// Reset the body reader after the request since at this point it's already read
			// Note that it's safe to ignore the error here since the 0,0 position is always valid
			_, _ = reader.Seek(0, 0)
		}
		return err
	}, retry.Attempts(3), retry.Delay(1*time.Second))

	return err
}

func (a *S3) PutAndCompress(ctx context.Context, key string, reader io.ReadSeeker, meta map[string]string, options ...PutOptions) error {
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	if meta == nil {
		meta = make(map[string]string)
	}

	encodedBytes := snappy.Encode(nil, data)
	meta["Compressor"] = "snappy"

	return a.Put(ctx, key, bytes.NewReader(encodedBytes), meta, options...)
}

func (a *S3) Del(ctx context.Context, key string) error {
	bucketName, key, err := a.getBucketAndKey(ctx, key)
	if err != nil {
		return err
	}

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}

	_, err = a.client.DeleteObjectWithContext(ctx, input)
	return err
}

func (a *S3) DelMulti(ctx context.Context, keys []string) error {
	bucketsNameKeys := make(map[string][]string)
	for _, k := range keys {
		bucketName, key, err := a.getBucketAndKey(ctx, k)
		if err != nil {
			return err
		}
		bucketsNameKeys[bucketName] = append(bucketsNameKeys[bucketName], key)
	}

	for bucketName, BKeys := range bucketsNameKeys {
		delObjects := make([]*s3.ObjectIdentifier, len(BKeys))

		for idx, key := range BKeys {
			delObjects[idx] = &s3.ObjectIdentifier{
				Key: aws.String(key),
			}
		}

		input := &s3.DeleteObjectsInput{
			Bucket: aws.String(bucketName),
			Delete: &s3.Delete{
				Objects: delObjects,
				Quiet:   aws.Bool(false),
			},
		}

		_, err := a.client.DeleteObjectsWithContext(ctx, input)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *S3) Head(ctx context.Context, key string, attributes []string) (map[string]string, error) {
	bucketName, key, err := a.getBucketAndKey(ctx, key)
	if err != nil {
		return nil, err
	}

	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}

	result, err := a.client.HeadObjectWithContext(ctx, input)
	if err != nil {
		if aerr, ok := err.(awserr.RequestFailure); ok {
			if aerr.StatusCode() == 404 {
				return nil, nil
			}
		}
		return nil, err
	}
	return getS3Meta(ctx, attributes, mergeHttpStandardHeaders(&HeadGetObjectOutputWrapper{
		headObjectOutput: result,
	})), nil
}

func (a *S3) ListObject(ctx context.Context, key string, prefix string, marker string, maxKeys int, delimiter string) ([]string, error) {
	bucketName, _, err := a.getBucketAndKey(ctx, key)
	if err != nil {
		return nil, err
	}

	input := &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
	}
	input.Prefix = aws.String(a.cfg.Prefix)
	if prefix != "" {
		input.Prefix = aws.String(a.cfg.Prefix + prefix)
	}
	input.Marker = aws.String(a.cfg.Prefix)
	if marker != "" {
		input.Marker = aws.String(a.cfg.Prefix + marker)
	}
	if maxKeys > 0 {
		input.MaxKeys = aws.Int64(int64(maxKeys))
	}
	if delimiter != "" {
		input.Delimiter = aws.String(delimiter)
	}

	result, err := a.client.ListObjectsWithContext(ctx, input)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0)
	for _, v := range result.Contents {
		keys = append(keys, *v.Key)
	}

	return keys, nil
}

func (a *S3) SignURL(ctx context.Context, key string, expired int64, options ...SignOptions) (string, error) {
	bucketName, key, err := a.getBucketAndKey(ctx, key)
	if err != nil {
		return "", err
	}
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	signOptions := DefaultSignOptions()
	for _, opt := range options {
		opt(signOptions)
	}
	if signOptions.process != nil {
		panic("process option is not supported for s3")
	}
	req, _ := a.client.GetObjectRequest(input)
	return req.Presign(time.Duration(expired) * time.Second)
}

func (a *S3) Exists(ctx context.Context, key string) (bool, error) {
	bucketName, key, err := a.getBucketAndKey(ctx, key)
	if err != nil {
		return false, err
	}

	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	_, err = a.client.HeadObjectWithContext(ctx, input)
	if err == nil {
		return true, nil
	}

	if aerr, ok := err.(awserr.RequestFailure); ok {
		if aerr.StatusCode() == 404 {
			return false, nil
		}
	}
	return false, err
}

func (a *S3) get(ctx context.Context, key string, options ...GetOptions) (*s3.GetObjectOutput, error) {
	bucketName, key, err := a.getBucketAndKey(ctx, key)
	if err != nil {
		return nil, err
	}

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	setS3Options(ctx, options, input)
	result, err := a.client.GetObjectWithContext(ctx, input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchKey {
				return nil, nil
			}
		}
		return nil, err
	}

	return result, nil
}

func getS3Meta(ctx context.Context, attributes []string, metaData map[string]*string) map[string]string {
	// https://github.com/aws/aws-sdk-go/issues/445
	// aws 会将 meta 的首字母大写，在这里需要转换下
	res := make(map[string]string)
	for _, v := range attributes {
		key := strings.Title(v)
		if metaData[key] != nil {
			res[v] = *metaData[key]
		}
	}
	return res
}

func setS3Options(ctx context.Context, options []GetOptions, getObjectInput *s3.GetObjectInput) {
	getOpts := DefaultGetOptions()
	for _, opt := range options {
		opt(getOpts)
	}
	if getOpts.contentEncoding != nil {
		getObjectInput.ResponseContentEncoding = getOpts.contentEncoding
	}
	if getOpts.contentType != nil {
		getObjectInput.ResponseContentType = getOpts.contentType
	}
}
