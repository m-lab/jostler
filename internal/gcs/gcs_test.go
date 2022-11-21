package gcs //nolint:testpackage

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"google.golang.org/api/option"
)

func TestVerbose(t *testing.T) { //nolint:paralleltest
	Verbose(func(fmt string, args ...interface{}) {})
}

func TestNewClient(t *testing.T) { //nolint:paralleltest
	saveStorageNewClient := storageNewClient
	defer func() {
		storageNewClient = saveStorageNewClient
	}()
	storageNewClient = testNewClient
	c := context.Background()
	// Should fail because context does not have deadline.
	if _, err := NewClient(c, "some-bucket"); !errors.Is(err, errCreateClient) {
		t.Fatalf("NewClient() = %v, want %v", err, errCreateClient)
	}
	ctx, cancel := context.WithTimeout(c, time.Second)
	defer cancel()
	// Should succeed because context does not have deadline.
	if _, err := NewClient(ctx, "some-bucket"); err != nil {
		t.Fatalf("NewClient() = %v, want nil", err)
	}
}

func testNewClient(ctx context.Context, opts ...option.ClientOption) (*storage.Client, error) {
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("forced failure") //nolint:goerr113
	}
	return &storage.Client{}, nil
}

func TestDownloadSucceed(t *testing.T) { //nolint:paralleltest
	gcsClient := fakeGCSClient()
	_, err := gcsClient.Download(context.Background(), "should-succeed")
	if err != nil {
		t.Fatalf("Download() = %v, want nil", err)
	}
}

func TestDownloadFail(t *testing.T) { //nolint:paralleltest
	gcsClient := fakeGCSClient()
	_, err := gcsClient.Download(context.Background(), "should-fail-new-reader")
	if !errors.Is(err, io.EOF) {
		t.Fatalf("Download() = %v, want %v", err, io.EOF)
	}

	gcsClient = fakeGCSClient()
	_, err = gcsClient.Download(context.Background(), "should-fail")
	if !errors.Is(err, errDownloadObject) {
		t.Fatalf("Download() = %v, want %v", err, errDownloadObject)
	}
}

func TestUploadSucceed(t *testing.T) { //nolint:paralleltest
	gcsClient := fakeGCSClient()
	err := gcsClient.Upload(context.Background(), "should-succeed", []byte("should-succeed"))
	if err != nil {
		t.Fatalf("Upload() = %v, want nil", err)
	}
}

func TestUploadFail(t *testing.T) { //nolint:paralleltest
	gcsClient := fakeGCSClient()
	err := gcsClient.Upload(context.Background(), "upload-contents", []byte("should-fail-write"))
	if !errors.Is(err, errUploadObject) {
		t.Fatalf("Upload() = %v, want %v", err, errUploadObject)
	}

	gcsClient = fakeGCSClient()
	err = gcsClient.Upload(context.Background(), "upload-contents", []byte("should-fail-close"))
	if !errors.Is(err, errCloseObject) {
		t.Fatalf("Upload() = %v, want %v", err, errCloseObject)
	}
}

type fakeClient struct {
	stiface.Client
}

func fakeGCSClient() *StorageClient {
	f := fakeClient{}
	return newStorageClient("some-bucket", &f, f.Bucket("some-bucket"))
}

func (f fakeClient) Bucket(name string) stiface.BucketHandle { //nolint:ireturn
	return &fakeBucketHandle{}
}

type fakeBucketHandle struct {
	stiface.BucketHandle
}

func (f fakeBucketHandle) Object(name string) stiface.ObjectHandle { //nolint:ireturn
	return fakeObjectHandle{name: name}
}

type fakeObjectHandle struct {
	stiface.ObjectHandle
	name string
}

func (f fakeObjectHandle) NewReader(ctx context.Context) (stiface.Reader, error) { //nolint:ireturn
	if f.name == "should-fail-new-reader" {
		return nil, io.EOF
	}
	return &fakeReader{data: []byte(f.name)}, nil
}

func (f fakeObjectHandle) NewWriter(ctx context.Context) stiface.Writer { //nolint:ireturn
	return &fakeWriter{}
}

// Fake reader implementation.
type fakeReader struct {
	stiface.Reader
	data  []byte
	index int
}

func (f *fakeReader) Close() error {
	return nil
}

func (f *fakeReader) Read(p []byte) (int, error) {
	if string(f.data) == "should-fail" {
		return 0, io.ErrUnexpectedEOF
	}
	if f.index >= len(f.data) {
		return 0, io.EOF
	}
	n := copy(p, f.data[f.index:])
	f.index += n
	return n, nil
}

// Fake writer implementation.
type fakeWriter struct {
	stiface.Writer
	data  []byte
	index int
}

func (f *fakeWriter) Close() error {
	if string(f.data) == "should-fail-close" {
		return io.EOF
	}
	return nil
}

func (f *fakeWriter) Write(p []byte) (int, error) {
	if string(p) == "should-fail-write" {
		return 0, io.ErrUnexpectedEOF
	}
	index := f.index
	f.data = append(f.data, p...)
	n := len(f.data) - index
	f.index = len(f.data)
	return n, nil
}
