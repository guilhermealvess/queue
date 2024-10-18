package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

var (
	SNS *sns.SNS
	SQS *sqs.SQS
)

func init() {
	os.Setenv("AWS_ACCESS_KEY_ID", "FAKE")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "FAKE")
	session, err := session.NewSession(aws.NewConfig().WithRegion("us-east-1").WithEndpoint("http://localhost:4566"))
	check(err)

	SNS = sns.New(session)
	SQS = sqs.New(session)
}

func main() {
	//fmt.Println("Running...")
	//start()

	args := os.Args
	switch args[1] {
	case "create":
		topicArn := args[2]
		content := readJSONFile(args[3])
		publishMessage(topicArn, content)

	case "list":
		topics := listTopics()
		for _, topic := range topics {
			fmt.Println(topic)
		}

	case "start":
		start()
	}
}

func start() {
	fmt.Println("Running...")
	db := NewDatabase()
	db.createTable()

	topics := listTopics()
	for _, topic := range topics {
		queueURL := createQueue(topic)
		go observer(db, topic, queueURL)
	}

	/* <-time.After(time.Second)
	_, err := SNS.Publish(&sns.PublishInput{
		Message:  aws.String("Hello, World!!!"),
		TopicArn: aws.String(topics[0]),
	})
	check(err) */
	for {
	}
}

func readJSONFile(file string) string {
	fileContent, err := os.ReadFile(file)
	check(err)
	return string(fileContent)
}

func publishMessage(topicArn, message string) {
	output, err := SNS.Publish(&sns.PublishInput{
		Message:  aws.String(message),
		TopicArn: aws.String(topicArn),
	})
	check(err)

	fmt.Println(*output.MessageId)
}

func listTopics() []string {
	output, err := SNS.ListTopics(&sns.ListTopicsInput{})
	check(err)

	var topics []string
	for _, topic := range output.Topics {
		topics = append(topics, *topic.TopicArn)
	}

	return topics
}

func createQueue(topic string) string {
	topicName := topic[strings.LastIndex(topic, ":")+1:]
	output, err := SQS.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(topicName),
	})

	check(err)

	queueArn := "arn:aws:sqs:us-east-1:000000000000:" + topicName
	_, err = SNS.Subscribe(&sns.SubscribeInput{
		Protocol: aws.String("sqs"),
		TopicArn: aws.String(topic),
		Endpoint: aws.String(queueArn),
	})

	check(err)

	return *output.QueueUrl
}

func observer(db *Database, topicArn, queueURL string) {
	for {
		output, err := SQS.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl: aws.String(queueURL),
		})

		check(err)

		for _, message := range output.Messages {
			db.saveMessage(*message.MessageId, topicArn, *message.Body)

			_, err = SQS.DeleteMessage(&sqs.DeleteMessageInput{
				QueueUrl:      aws.String(queueURL),
				ReceiptHandle: message.ReceiptHandle,
			})

			check(err)
		}
	}
}

type Database struct {
	*sqlx.DB
}

func NewDatabase() *Database {
	db, err := sqlx.Open("sqlite3", "queue.db")
	check(err)

	return &Database{db}
}

func (db *Database) createTable() {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS messages (
		message_id VARCHAR PRIMARY KEY,
		topic_arn VARCHAR NOT NULL,
		message TEXT,
		timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`)
	check(err)
}

func (db *Database) saveMessage(messageID, topicARN, message string) {
	_, err := db.Exec(`INSERT INTO messages (message_id, topic_arn, message) VALUES ($1, $2, $3)`, messageID, topicARN, message)
	check(err)
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
