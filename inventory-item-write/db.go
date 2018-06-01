package main

import (
	"log"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	"github.com/BranislavLazic/go-aws-es-cqrs/inventory-item-write/proto"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var db = dynamodb.New(session.New(), aws.NewConfig().WithRegion("eu-central-1"))

const eventStoreTableName = "InventoryEvents"

type SerializableEvent interface {
	serialize() []byte
}

type ItemAdded struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}
type ItemUpdated ItemAdded
type ItemDeleted struct {
	ID string `json:"id"`
}

// Serialization
func (ia ItemAdded) serialize() []byte {
	itemAddedEvt := &itemEvents.ItemAdded{
		Id:   ia.ID,
		Name: ia.Name,
	}
	return marshalToProtobuf(itemAddedEvt)
}

func (iu ItemUpdated) serialize() []byte {
	itemUpdatedEvt := &itemEvents.ItemUpdated{
		Id:   iu.ID,
		Name: iu.Name,
	}
	return marshalToProtobuf(itemUpdatedEvt)
}

func (id ItemDeleted) serialize() []byte {
	itemDeletedEvt := &itemEvents.ItemDeleted{
		Id: id.ID,
	}
	return marshalToProtobuf(itemDeletedEvt)
}

func marshalToProtobuf(event proto.Message) []byte {
	data, err := proto.Marshal(event)
	if err != nil {
		log.Fatal("marshalling failed: ", err)
	}
	return data
}

//-------------------------------------------------------

type EventStoreRecord struct {
	// Partition key
	ID string `json:"id"`
	// Sort key
	Sequence  int    `json:"sequence"`
	Tag       string `json:"tag"`
	Timestamp string `json:"timestamp"`
	Event     string `json:"event"`
}

func newEventStoreRecord(id, tag string, event SerializableEvent) *EventStoreRecord {
	return &EventStoreRecord{
		ID:        id,
		Sequence:  0,
		Tag:       tag,
		Timestamp: time.Now().String(),
		Event:     string(event.serialize()),
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
	if err = dynamodbattribute.UnmarshalMap(result.Item, eventStoreRecord); err != nil {
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
