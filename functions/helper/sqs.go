package helper

import (
	"encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type SQSHelper interface {
	SendMessage(queueURL string, message string) (*sqs.SendMessageOutput, error)
	GetQueueURL(queue string) (*sqs.GetQueueUrlOutput, error)
	GetMessages(queueURL string) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageBatch(input *sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error)
	SendMessageBatch(input *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error)
	SendMessageDelay(queueURL string, message map[string]interface{}, delaySeconds int64) (*sqs.SendMessageOutput, error)
}

type sqsHelper struct {
	sqsSvc *sqs.SQS
}

func NewSQS(awsRegion string) SQSHelper {
	sqsSession, err := session.NewSession(&aws.Config{Region: aws.String(awsRegion)})
	if err != nil {
		panic(err)
	}
	sqsSvc := sqs.New(sqsSession)
	return &sqsHelper{
		sqsSvc: sqsSvc,
	}
}

// SendMessage represent to send SQS message
func (h *sqsHelper) SendMessage(queueURL string, message string) (*sqs.SendMessageOutput, error) {
	// ! double json encode
	jsonMsg, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}

	result, err := h.sqsSvc.SendMessage(&sqs.SendMessageInput{
		MessageBody: aws.String(string(jsonMsg)),
		QueueUrl:    aws.String(queueURL),
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

// GetQueueURL represents URL of the queue we want to send a message to
func (h *sqsHelper) GetQueueURL(queue string) (*sqs.GetQueueUrlOutput, error) {
	result, err := h.sqsSvc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &queue,
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

// GetMessages represent to get SQS message
func (h *sqsHelper) GetMessages(queueURL string) (*sqs.ReceiveMessageOutput, error) {
	msgResult, err := h.sqsSvc.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl: &queueURL,
	})
	if err != nil {
		return nil, err
	}

	return msgResult, nil
}

// DeleteMessageBatch represent to delete SQS message
func (h *sqsHelper) DeleteMessageBatch(input *sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error) {
	msgResult, err := h.sqsSvc.DeleteMessageBatch(input)
	if err != nil {
		return nil, err
	}

	return msgResult, nil
}

// SendMessageBatch represents to send batch of message to SQS
func (h *sqsHelper) SendMessageBatch(input *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
	msgResult, err := h.sqsSvc.SendMessageBatch(input)
	if err != nil {
		return nil, err
	}

	return msgResult, nil
}

// SendMessageDelay represent to send message to SQS with delay seconds
func (h *sqsHelper) SendMessageDelay(queueURL string, message map[string]interface{}, delaySeconds int64) (*sqs.SendMessageOutput, error) {
	// ! double json encode
	jsonMsg, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}

	result, err := h.sqsSvc.SendMessage(&sqs.SendMessageInput{
		MessageBody:  aws.String(string(jsonMsg)),
		QueueUrl:     aws.String(queueURL),
		DelaySeconds: &delaySeconds,
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}
