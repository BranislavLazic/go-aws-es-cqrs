package main

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/twinj/uuid"
)

func createItem(req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	eventStoreRecord := NewEventStoreRecord(uuid.NewV4().String(), "add-item", req.Body)
	err := eventStoreRecord.persist()
	if err != nil {
		return serverError(err)
	}
	return events.APIGatewayProxyResponse{StatusCode: 201}, nil
}

func clientError() (events.APIGatewayProxyResponse, error) {
	return events.APIGatewayProxyResponse{
		StatusCode: 405,
	}, nil
}

func serverError(err error) (events.APIGatewayProxyResponse, error) {
	return events.APIGatewayProxyResponse{
		Body:       err.Error(),
		StatusCode: 500,
	}, nil
}

func router(req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	switch req.HTTPMethod {
	case "POST":
		return createItem(req)
	default:
		return clientError()
	}
}