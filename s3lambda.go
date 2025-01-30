package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Payload struct {
	FileName    string `json:"fileName"`
	FileSize    int64  `json:"fileSize"`
	FileType    string `json:"fileType"`
	ContentType string `json:"contentType"`
}

func handler(ctx context.Context, s3Event events.S3Event) error {
	// Create AWS session once, outside the loop
	sess := session.Must(session.NewSession())
	svc := s3.New(sess)
	client := &http.Client{}
	apiUrl := os.Getenv("API_URL")
	if apiUrl == "" {
		log.Fatal("Error loading API_URL is not defined")
	}
	apiKey := os.Getenv("API_KEY")
	if apiKey == "" {
		log.Fatal("Error loading API_KEY is not defined")
	}
	for _, record := range s3Event.Records {
		s3Entity := record.S3
		bucket := s3Entity.Bucket.Name
		key := s3Entity.Object.Key
		region := record.AWSRegion

		log.Printf("Processing: Bucket: %s, Key: %s, Region: %s", bucket, key, region)

		// Update session region for current record
		svc.Config.Region = aws.String(region)

		// Get the file metadata
		headObjectOutput, err := svc.HeadObject(&s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			log.Printf("Error getting object metadata: %v", err)
			continue // Continue processing other records
		}

		// // Extract TransactionUuid from the object key
		// parts := strings.Split(key, "_")
		// if len(parts) == 0 {
		// 	log.Printf("Invalid key format: %s", key)
		// 	continue
		// }

		payload := Payload{
			FileName: key,
			FileSize: *headObjectOutput.ContentLength,
			FileType: *headObjectOutput.ContentType,
		}

		// Make the PUT request to the API
		jsonPayload, err := json.Marshal(payload)
		if err != nil {
			log.Printf("Error marshaling payload: %v", err)
			continue
		}

		log.Printf("Payload: %s", string(jsonPayload))

		// req, err := http.NewRequestWithContext(ctx, "PUT", "http://20.199.75.58:8080/v1/fileUploaded",
		req, err := http.NewRequestWithContext(ctx, "PUT", apiUrl,
			strings.NewReader(string(jsonPayload)))
		if err != nil {
			log.Printf("Error creating request: %v", err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("apiKey", apiKey)

		resp, err := client.Do(req)
		if err != nil {
			log.Printf("Error making request: %v", err)
			continue
		}

		log.Printf("Response from API: %d - %s", resp.StatusCode, resp.Status)
		resp.Body.Close()
	}

	return nil
}

func main() {
	lambda.Start(handler)
}
