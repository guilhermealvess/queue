#!/bin/bash

# Create SNS topics
SHIPMENT_TOPIC="envios_shipment"
awslocal --endpoint-url http://localhost:4566 sns create-topic --name "$SHIPMENT_TOPIC"