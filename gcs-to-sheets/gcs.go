package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"cloud.google.com/go/storage"
)

// https://cloud.google.com/storage/docs/downloading-objects#storage-download-object-go
// gcsdownloadFile downloads an object to a file.
func gcsDownloadFile(bucket, object string) (filebyte []byte, err error) {
	log.Printf("Trying to get object %s from bucket %s", object, bucket)

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Printf("could not form storage client %v", err)
		return nil, fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		log.Printf("Object(%q).NewReader: %v", object, err)
		return nil, fmt.Errorf("Object(%q).NewReader: %v", object, err)
	}
	defer rc.Close()

	i, err := io.ReadAll(rc)
	if err != nil {
		log.Printf("could not read the object : %v", err)
		return nil, fmt.Errorf("could not read the object : %v", err)
	}

	return i, nil
}
