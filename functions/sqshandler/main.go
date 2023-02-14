package main

import (
	"encoding/json"
	"fmt"
	"go-lambda-sqs-example/functions/common"
	"go-lambda-sqs-example/functions/util"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func main() {
	lambda.Start(func(sqsEvent events.SQSEvent) (string, error) {
		err := run(sqsEvent)
		if err != nil {
			return "ERROR", fmt.Errorf("ERROR: %+v", err)
		}

		return "OK", nil
	})
}

func run(sqsEvent events.SQSEvent) error {
	if len(sqsEvent.Records) == 0 {
		fmt.Println("No message in SQS")
		return nil
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

	// handle message get from SQS
	processedReceiptHandles := make([]*sqs.DeleteMessageBatchRequestEntry, 0, len(sqsEvent.Records))
	for _, mess := range sqsEvent.Records {
		intf := make(map[string]interface{})
		if err := json.Unmarshal([]byte(mess.Body), &intf); err != nil {
			return err
		}

		valueType, ok := intf["type"]
		if ok {
			typeMessage := valueType.(string)
			switch {
			case strings.EqualFold(typeMessage, "cronjob"):
				fmt.Println("do process execute message")
			}
		}
		processedReceiptHandles = append(processedReceiptHandles, &sqs.DeleteMessageBatchRequestEntry{
			Id:            aws.String(mess.MessageId),
			ReceiptHandle: &mess.ReceiptHandle,
		})
	}

	// delete message in SQS
	if len(processedReceiptHandles) > 0 {
		deleteMessageRequest := sqs.DeleteMessageBatchInput{
			QueueUrl: urlRes.QueueUrl,
			Entries:  processedReceiptHandles,
		}

		_, err = util.DeleteMessageBatch(sqsSvc, &deleteMessageRequest)
		if err != nil {
			fmt.Println("failed to delete message batch with err: ", err)
			return err
		}
	}
	return nil
}
