package s3log

import (
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3WAL struct {
	client     *s3.Client
	bucketName string
	length     uint64
}

func (w *S3WAL) append(ctx context.Context, data []byte) (uint64, error) {
	nextOffset := w.length + 1

	input := &s3.PutObjectInput{
		Bucket:      aws.String(w.bucketName),
		Key:         aws.String(fmt.Sprintf("%020d", nextOffset)),
		Body:        bytes.NewReader(data),
		IfNoneMatch: aws.String(""),
	}

	if _, err := w.client.PutObject(ctx, input); err != nil {
		return 0, fmt.Errorf("failed to put object to S3: %w", err)
	}
	w.length = nextOffset
	return nextOffset, nil
}
