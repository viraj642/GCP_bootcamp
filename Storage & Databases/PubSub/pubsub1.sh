#!/bin/bash


if [[ $# -gt 0 ]]
then
	keyPath=$1
	topicName=$2
	sub1Name=$3
	sub2Name=$4
else
	keyPath="keys/pubsubSA_key.json"
	topicName="topic1"
	sub1Name="sub1"
	sub2Name="sub2"
fi

# Activate the user-created service account
gcloud auth activate-service-account --key-file=$keyPath

# Create topic 
gcloud pubsub topics create $topicName

# Create pull subscription
gcloud pubsub subscriptions create $sub1Name --topic=$topicName

if [[ ${#sub2Name} -gt 0 ]]
then
	gcloud pubsub subscriptions create $sub2Name --topic=$topicName
fi
