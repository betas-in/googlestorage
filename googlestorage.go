package googlestorage

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"github.com/betas-in/logger"
)

// GCStorage ...
type GCStorage interface {
	Upload(path, object string) error
	Download(object, path string) (string, error)
	Exists(object string) (bool, error)
	Close() error
}

type gcStorage struct {
	bucket  string
	timeout time.Duration
	client  *storage.Client
	log     *logger.Logger
}

// NewGCStorage ...
func NewGCStorage(bucket string, timeout time.Duration, log *logger.Logger) (GCStorage, error) {
	up := gcStorage{}
	up.bucket = bucket
	up.timeout = timeout
	up.log = log

	ctx, cancel := context.WithTimeout(context.Background(), up.timeout)
	defer cancel()

	gac := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if gac == "" {
		return nil, fmt.Errorf("GOOGLE_APPLICATION_CREDENTIALS were not found in the environment")
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	up.client = client

	return &up, nil
}

func (u *gcStorage) Upload(path, object string) error {
	if path == "" {
		err := fmt.Errorf("path cannot be empty")
		return err
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	ctx, cancel := context.WithTimeout(context.Background(), u.timeout)
	defer cancel()

	bucket := u.client.Bucket(u.bucket)
	obj := bucket.Object(object)
	writer := obj.NewWriter(ctx)

	_, err = io.Copy(writer, file)
	if err != nil {
		return err
	}

	err = writer.Close()
	if err != nil {
		return err
	}

	return nil
}

func (u *gcStorage) Download(object, path string) (string, error) {
	if object == "" || path == "" {
		err := fmt.Errorf("object or path cannot be empty")
		return "", err
	}
	file, err := os.CreateTemp("", path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	ctx, cancel := context.WithTimeout(context.Background(), u.timeout)
	defer cancel()

	bucket := u.client.Bucket(u.bucket)
	obj := bucket.Object(object)
	reader, err := obj.NewReader(ctx)
	if err == storage.ErrObjectNotExist {
		return "", nil
	}
	if err != nil {
		_ = os.Remove(path)
		return "", err
	}

	if _, err = io.Copy(file, reader); err != nil {
		_ = os.Remove(path)
		return "", err
	}

	if err := reader.Close(); err != nil {
		return "", err
	}

	return file.Name(), nil
}

func (u *gcStorage) Exists(object string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), u.timeout)
	defer cancel()

	bucket := u.client.Bucket(u.bucket)
	obj := bucket.Object(object)
	_, err := obj.NewReader(ctx)
	if err == storage.ErrObjectNotExist {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (u *gcStorage) GetSignedURL() {
	// opts := storage.SignedURLOptions{}
	// storage.SignedURL("bucket-name", "object-name", &opts)
}

func (u *gcStorage) Close() error {
	return u.client.Close()
}
