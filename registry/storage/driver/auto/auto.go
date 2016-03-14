
package auto

import (
	"io"
	"io/ioutil"
	"sync"


	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
	"bytes"
	"fmt"
	//"strconv"
)

const driverName = "auto"

// minChunkSize defines the minimum multipart upload chunk size
// S3 API requires multipart upload chunks to be at least 5MB
const minChunkSize = 5 << 20

const defaultChunkSize = 2 * minChunkSize

// listMax is the largest amount of objects you can request from S3 in a list call
const listMax = 1000




func init() {
	fmt.Println("-----------autohome  fastdfs init---------------")
	factory.Register(driverName, &autoDriverFactory{})
}

// autoDriverFactory implements the factory.StorageDriverFactory interface
type autoDriverFactory struct{}

func (factory *autoDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}




type driver struct {
	ChunkSize     int64
	pool  sync.Pool // pool []byte buffers used for WriteStream
	zeros []byte    // shared, zero-valued buffer used for WriteStream
}

type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by Autohome fastdfs
// Objects are stored at absolute keys in the provided bucket.
type Driver struct {
	baseEmbed
}

// FromParameters constructs a new Driver with a given parameters map
func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	return New()
}

// New constructs a new Driver with the given AWS credentials, region, encryption flag, and
// bucketName
func New() (*Driver, error) {
	chunkSize := int64(defaultChunkSize)
	d := &driver{
		ChunkSize:     chunkSize,
		zeros:         make([]byte, chunkSize),
	}
	d.pool.New = func() interface{} {
		return make([]byte, d.ChunkSize)
	}
	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: d,
			},
		},
	}, nil
}

// Implement the storagedriver.StorageDriver interface

func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	rc, err := d.ReadStream(ctx, path, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	p, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}

	return p, nil
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {
	if _, err := d.WriteStream(ctx, path, 0, bytes.NewReader(contents)); err != nil {
		return err
	}
	return nil
}

// ReadStream retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) ReadStream(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	fdfs := NewFdfs(path,offset);
	//判断offset是否超出范围
	c,err := fdfs.Checkoffset();
	if err!=nil || !c {
		//fmt.Println("------->"+path+" offset="+strconv.FormatInt(offset,10)+"  offset wrong")
		return nil,err
	}
	return fdfs,nil
}

// WriteStream stores the contents of the provided io.Reader at a
// location designated by the given path. The driver will know it has
// received the full contents when the reader returns io.EOF. The number
// of successfully READ bytes will be returned, even if an error is
// returned. May be used to resume writing a stream by providing a nonzero
// offset. Offsets past the current size will write from the position
// beyond the end of the file.
func (d *driver) WriteStream(ctx context.Context, path string, offset int64, reader io.Reader) (totalRead int64, err error) {
	fdfs := NewFdfs(path,offset)
	return fdfs.Write(reader)
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	fdfs := NewFdfs(path,int64(0));
	return fdfs.Stat();
}

// List returns a list of the objects that are direct descendants of the given path.
func (d *driver) List(ctx context.Context, opath string) ([]string, error) {
	fdfs := NewFdfs(opath,int64(0));
	return fdfs.List();
}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	fdfs := NewFdfs(sourcePath,int64(0));
	return fdfs.Move(destPath);
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, path string) error {
	fdfs := NewFdfs(path,int64(0));
	return fdfs.Delete();
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	return "",storagedriver.ErrUnsupportedMethod{}
}


