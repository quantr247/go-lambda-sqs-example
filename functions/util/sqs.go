package util

import (
	"encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// SendMessage represent to send SQS message
func SendMessage(sqsSvc *sqs.SQS, queueURL string, message string) (*sqs.SendMessageOutput, error) {
	// ! double json encode
	jsonMsg, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}

	result, err := sqsSvc.SendMessage(&sqs.SendMessageInput{
		MessageBody: aws.String(string(jsonMsg)),
		QueueUrl:    aws.String(queueURL),
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

// GetQueueURL represents URL of the queue we want to send a message to
func GetQueueURL(sqsSvc *sqs.SQS, queue string) (*sqs.GetQueueUrlOutput, error) {
	result, err := sqsSvc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &queue,
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

// GetMessages represent to get SQS message
func GetMessages(sqsSvc *sqs.SQS, queueURL string) (*sqs.ReceiveMessageOutput, error) {
	msgResult, err := sqsSvc.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl: &queueURL,
	})
	if err != nil {
		return nil, err
	}

	return msgResult, nil
}

// DeleteMessageBatch represent to delete SQS message
func DeleteMessageBatch(sqsSvc *sqs.SQS, input *sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error) {
	msgResult, err := sqsSvc.DeleteMessageBatch(input)
	if err != nil {
		return nil, err
	}

	return msgResult, nil
}

// SendMessageBatch represents to send batch of message to SQS
func SendMessageBatch(sqsSvc *sqs.SQS, input *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
	msgResult, err := sqsSvc.SendMessageBatch(input)
	if err != nil {
		return nil, err
	}

	return msgResult, nil
}
