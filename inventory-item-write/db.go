package main

import (
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var db = dynamodb.New(session.New(), aws.NewConfig().WithRegion("eu-central-1"))

const eventStoreTableName = "InventoryEvents"

type ItemAdded struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}
type NameUpdated ItemAdded
type ItemDeleted struct {
	ID string `json:"id"`
}

type EventStoreRecord struct {
	// Partition key
	ID string `json:"id"`
	// Sort key
	Sequence  int    `json:"sequence"`
	Tag       string `json:"tag"`
	Timestamp string `json:"timestamp"`
	Event     string `json:"event"`
}

func NewEventStoreRecord(id, tag, event string) *EventStoreRecord {
	return &EventStoreRecord{
		ID:        id,
		Sequence:  0,
		Tag:       tag,
		Timestamp: time.Now().String(),
		Event:     event,
	}
}

func (esr EventStoreRecord) recover() (*EventStoreRecord, error) {
	input := &dynamodb.GetItemInput{
		TableName: aws.String(eventStoreTableName),
		Key: map[string]*dynamodb.AttributeValue{
			"Id": {
				S: aws.String(esr.ID),
			},
		},
	}
	result, err := db.GetItem(input)
	if err != nil {
		return nil, err
	}
	eventStoreRecord := &EventStoreRecord{}
	err = dynamodbattribute.UnmarshalMap(result.Item, eventStoreRecord)
	if err != nil {
		return nil, err
	}
	return eventStoreRecord, nil
}

func (esr EventStoreRecord) persist() error {
	input := &dynamodb.PutItemInput{
		TableName: aws.String(eventStoreTableName),
		Item: map[string]*dynamodb.AttributeValue{
			"Id":        {S: aws.String(esr.ID)},
			"Sequence":  {N: aws.String(strconv.Itoa(esr.Sequence + 1))},
			"Tag":       {S: aws.String(esr.Tag)},
			"Timestamp": {S: aws.String(esr.Timestamp)},
			"Event":     {S: aws.String(esr.Event)},
		},
	}
	_, err := db.PutItem(input)
	return err
}
