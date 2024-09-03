# EOSS: Wrapper For Aliyun OSS And Amazon S3

awos for node: https://github.com/shimohq/awos-js

## Features

- enable shards bucket
- add retry strategy
- avoid 404 status code:
  - `Get(objectName string) (string, error)` will return `"", nil` when object not exist
  - `Head(key string, meta []string) (map[string]string, error)` will return `nil, nil` when object not exist

## Installing

Use go get to retrieve the SDK to add it to your GOPATH workspace, or project's Go module dependencies.

```bash
go get github.com/ego-component/eos
```

## How to use
### config
```toml
[storage]
storageType = "oss" # oss|s3
accessKeyID = "xxx"
accessKeySecret = "xxx"
endpoint = "oss-cn-beijing.aliyuncs.com"
bucket = "aaa" # 定义默认storage实例
shards = []
  # 定义其他storage实例
  [storage.buckets.template] 
  bucket = "template-bucket"
  shards = []
  [storage.buckets.fileContent]
  bucket = "contents-bucket"
  shards = [
   "abcdefghijklmnopqr",
   "stuvwxyz0123456789"
  ]
```

```golang
import "github.com/ego-component/eos"

// 构建 os component
cmp := eoss.Load("storage").Build()
```

Available operations：

```golang
Get(ctx context.Context, key string, options ...GetOptions) (string, error)
GetBytes(ctx context.Context, key string, options ...GetOptions) ([]byte, error)
GetAsReader(ctx context.Context, key string, options ...GetOptions) (io.ReadCloser, error)
GetWithMeta(ctx context.Context, key string, attributes []string, options ...GetOptions) (io.ReadCloser, map[string]string, error)
Put(ctx context.Context, key string, reader io.ReadSeeker, meta map[string]string, options ...PutOptions) error
Del(ctx context.Context, key string) error
DelMulti(ctx context.Context, keys []string) error
Head(ctx context.Context, key string, meta []string) (map[string]string, error)
ListObject(ctx context.Context, key string, prefix string, marker string, maxKeys int, delimiter string) ([]string, error)
SignURL(ctx context.Context, key string, expired int64) (string, error)
GetAndDecompress(ctx context.Context, key string) (string, error)
GetAndDecompressAsReader(ctx context.Context, key string) (io.ReadCloser, error)
CompressAndPut(ctx context.Context, key string, reader io.ReadSeeker, meta map[string]string, options ...PutOptions) error
Range(ctx context.Context, key string, offset int64, length int64) (io.ReadCloser, error)
Exists(ctx context.Context, key string)(bool, error)
```

https://aws.github.io/aws-sdk-go-v2/docs/migrating/