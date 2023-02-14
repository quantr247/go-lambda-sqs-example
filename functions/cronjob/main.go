package main

import (
	"context"
	"encoding/json"
	"fmt"
	"go-lambda-sqs-example/functions/common"
	"go-lambda-sqs-example/functions/util"
	"strconv"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type CronData struct {
	ID    int
	Value string
}

func main() {
	lambda.Start(func() (string, error) {
		if err := run(nil); err != nil {
			return "ERROR", fmt.Errorf("ERROR: %+v", err)
		}
		return "OK", nil
	})
}

func run(args map[string]interface{}) error {
	ctx := context.Background()
	// fetch data
	listData, err := getData(ctx)
	if err != nil {
		return err
	}

	// init SQS service
	sqsSession, err := session.NewSession(&aws.Config{Region: aws.String(common.AWSRegion)})
	if err != nil {
		return err
	}
	sqsSvc := sqs.New(sqsSession)
	urlRes, err := util.GetQueueURL(sqsSvc, common.SQSName)
	if err != nil {
		return err
	}

	// send batch message to SQS
	batchMessageData := make([]*sqs.SendMessageBatchRequestEntry, 0, common.MaximumSQSBatchMessage)
	for i, data := range listData {
		message := map[string]interface{}{
			"type":  "cronjob",
			"id":    data.ID,
			"value": data.Value,
		}

		jsonMsg, err := json.Marshal(message)
		if err != nil {
			return err
		}

		messageRequest := &sqs.SendMessageBatchRequestEntry{
			Id:          aws.String(strconv.FormatInt(int64(data.ID), 10)),
			MessageBody: aws.String(string(jsonMsg)),
		}

		batchMessageData = append(batchMessageData, messageRequest)

		// Send batch message when batch size = maximum batch message or when user is the last of listData
		if len(batchMessageData) == common.MaximumSQSBatchMessage || i == (len(listData)-1) {
			sendMessageRequest := sqs.SendMessageBatchInput{
				QueueUrl: urlRes.QueueUrl,
				Entries:  batchMessageData,
			}

			_, err = sqsSvc.SendMessageBatch(&sendMessageRequest)
			if err != nil {
				return err
			}

			// Remove all in batchMessageData to append next data
			batchMessageData = batchMessageData[:0]
		}
	}
	return nil
}

// getData represent for fetching data to execute in schedule time
func getData(ctx context.Context) ([]CronData, error) {
	var (
		listData []CronData
	)

	for i := 0; i < 100; i++ {
		data := CronData{
			ID:    i,
			Value: fmt.Sprintf("Value number %v", i),
		}
		listData = append(listData, data)
	}
	return listData, nil
}
