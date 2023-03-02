#!/bin/bash

# Creste the google cloud stroage bucket with user created service account using authentication key with versioning
# And object lifecycle - objects to be deleted in 15 Days

# Define the bucket name, bucket class,  location/region, path of the service account key and path of the json file for object lifecycle
if [[ $# -gt 1 ]]
then
	BucketName=$1
    BucketClass=$2
    Region=$3
    KeyPath=$4
    LifecycleJsonFilePath=$5
else
	BucketName='viraj-patil-fagcpbcmp'
	BucketClass='standard'
	Region='us-central1'
	KeyPath='keys/gsa_keyfile.json'
	LifecycleJsonFilePath='gcs_lifecycle.json'
fi

# Activate the user-created service account
gcloud auth activate-service-account --key-file=$KeyPath

# Create the bucket
gsutil mb -c $BucketClass -l $Region gs://$BucketName

# Enable object versioning
gsutil versioning set on gs://$BucketName

# Set object lifecycle to delete after 15 days
gsutil lifecycle set $LifecycleJsonFilePath gs://$BucketName

