package common

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type ObjectPath struct {
	Bucket string
	Key    string
}

type Object struct {
	path ObjectPath
	body *bytes.Reader
}

type IStorage interface {
	Get() (io.ReadCloser, error)
	Put() error
}

type Storage struct {
	ctx    context.Context
	client *s3.Client
}

func (s Storage) Get(path ObjectPath) (io.ReadCloser, error) {
	output, err := s.client.GetObject(s.ctx, &s3.GetObjectInput{
		Bucket: aws.String(path.Bucket),
		Key:    aws.String(path.Key),
	})

	if err != nil {
		return nil, fmt.Errorf("common:objectStorage:storage -> %v", err)
	}

	return output.Body, nil
}

func (s Storage) Put(obj Object) error {
	_, err := s.client.PutObject(s.ctx, &s3.PutObjectInput{
		Bucket: aws.String(obj.path.Bucket),
		Key:    aws.String(obj.path.Key),
		Body:   obj.body,
	})

	if err != nil {
		return err
	}

	return nil
}

func NewStorage(ctx context.Context, cfg aws.Config) *Storage {
	return &Storage{client: s3.NewFromConfig(cfg), ctx: ctx}
}
