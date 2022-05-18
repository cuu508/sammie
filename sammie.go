package main

import (
	"context"
	"fmt"
	"log"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"os"
	"strings"
)

var NUM_WORKERS = 10
var ENDPOINT = os.Getenv("ENDPOINT")
var REGION = os.Getenv("REGION")
var ACCESS_KEY = os.Getenv("ACCESS_KEY")
var SECRET_KEY = os.Getenv("SECRET_KEY")
var BUCKET = os.Getenv("BUCKET")
var DST = os.Getenv("DST")

var jobs chan string
var exists = struct{}{}

func makeclient() *minio.Client {
	result, err := minio.New(ENDPOINT, &minio.Options{
		Region: REGION,
		Creds:  credentials.NewStaticV4(ACCESS_KEY, SECRET_KEY, ""),
		Secure: true,
	})
	if err != nil {
		log.Fatalln(err)
	}
	return result
}

func good(path string, size int64) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}

	if info.Size() != size {
		return false
	}

	return true
}

func splitname(path string) string {
	_, name, found := strings.Cut(path, "/")
	if !found {
		log.Fatalln("Unexpected key: ", path)
	}
	return name
}

func worker(ctx context.Context, done chan bool) {
	client := makeclient()
	for key := range jobs {
		log.Printf("[%s] * %s", ctx.Value("label"), key)

		remoteset := make(map[string]struct{})
		objs := client.ListObjects(ctx, BUCKET, minio.ListObjectsOptions{
			Prefix: key,
		})

		// Upload
		for obj := range objs {
			if obj.Err != nil {
			    log.Fatalln(obj.Err)
			}

			remoteset[splitname(obj.Key)] = exists
			dst := DST + obj.Key
			if good(dst, obj.Size) {
				continue;
			}

			log.Printf("[%s] + %s", ctx.Value("label"), obj.Key)
			err := client.FGetObject(ctx, BUCKET, obj.Key, dst, minio.GetObjectOptions{})
			if err != nil && err.Error() != "The specified key does not exist." {
			    log.Fatalln(err)
			}
		}

		// Delete
		files, err := os.ReadDir(DST + key)
		if err != nil {
			log.Fatal(err)
		}

		for _, file := range files {
			if _, ok := remoteset[file.Name()]; !ok {
			    path := key + "/" + file.Name()
    			log.Printf("[%s] - %s", ctx.Value("label"), path)
			    os.Remove(DST + path)
			}
		}
	}
	done <- true
}

func main() {
	jobs = make(chan string, 5000)

	// Start workers
	ctx := context.Background()
	results := make(chan bool, NUM_WORKERS)
	for i := 1; i <= NUM_WORKERS; i++ {
		wctx := context.WithValue(ctx, "label", fmt.Sprintf("w%02d", i))
		go worker(wctx, results)
	}

	// List top-level directories
	dirs := makeclient().ListObjects(ctx, BUCKET, minio.ListObjectsOptions{})
	for dir := range dirs {
		if dir.Err != nil {
		    log.Fatalln(dir.Err)
		}

	    jobs <- dir.Key
	}

	close(jobs)
	log.Println("Waiting for workers...")
	for i := 1; i <= NUM_WORKERS; i++ {
		<-results
	}

	log.Println("Bye")
}
