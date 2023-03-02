from google.cloud import pubsub_v1
from google.oauth2 import service_account
import json
import time
import sys

# set up credentials
if len(sys.argv) == 3:
    credentials_path = sys.argv[1]
    subscription_id = sys.argv[1]
else:    
    credentials_path = "keys/pubsubSA_key.json"
    subscription_id="sub1"

def authenticateServiceAccount(key_path):
    try:
        credentials = service_account.Credentials.from_service_account_file(
            key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return credentials
    except Exception as e:
        print(f"Failed to authenticate with service account key file: {e}")
        return None

credentials = authenticateServiceAccount(credentials_path) 

try:

    # set up Pub/Sub client and subscriber
    project_id = "viraj-patil-bootcamp"
    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    # define callback function to process incoming messages
    def callback(message):
        # convert message to dictionary object
        data = json.loads(message.data.decode("utf-8"))
        
        # write dictionary object to file as JSON
        with open("msg_from_sub1.json", "a") as outfile:
            json.dump(data, outfile)
            outfile.write("\n")
        
        # acknowledge receipt of message
        message.ack()

    # start subscriber and wait for incoming messages
    subscriber.subscribe(subscription_path, callback=callback)

except Exception as e:
    print(f"Exception occured in process: {e}")


print("Listening for messages on subscription {}. Press Ctrl+C to exit.".format(subscription_path))
while True:
    try:
        time.sleep(5)
    except KeyboardInterrupt:
        print("Exiting...")
        break

