#!/bin/bash

#Createing the compute instance of debian image in project and zone as mentioned in below cammand
#And vpc network = vpc-bootcamp with no external ip address.

#Define variable which are project name, zone, new vm name and existing subnet name
if [[ $# -eq 6 ]]
then
    projectName=$1
    Zone=$2
    vmName=$3
    Network=$4
    subnet=$5
    machineType=$6
else
    echo "1:$projectName,2:$Zone,3:$vmName,4:$Network,5:$subnet,6:$machineType"
    echo "Enter the all Arguments project name, zone, vm name, network name, subnet name, machine type(e.g. f1-micro, n1-standard-2 etc.) "
fi

gcloud compute instances create $vmName \
    --project=$projectName \
    --zone=$Zone \
    --machine-type=$machineType \
    --network-interface=network=$Network,subnet=$subnet,no-address \
    --scopes=https://www.googleapis.com/auth/cloud-platform  \
    --image=debian-11-bullseye-v20221206 \
    --image-project=debian-cloud