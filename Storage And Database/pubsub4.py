from google.cloud import pubsub_v1
from google.oauth2 import service_account
import json
import time

# set up credentials
credentials_path = "keys/pubsubSA_key.json"
creds = service_account.Credentials.from_service_account_file(credentials_path)

# set up Pub/Sub client and subscriber
project_id = "viraj-patil-bootcamp"
subscription_id="sub2"
subscriber = pubsub_v1.SubscriberClient(credentials=creds)
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# define callback function to process incoming messages
def callback(message):
    # convert message to dictionary object
    data = json.loads(message.data.decode("utf-8"))
    # write dictionary object to file as JSON
    with open("msg_from_sub2.json", "a") as outfile:
        json.dump(data, outfile)
        outfile.write("\n")
    
    # acknowledge receipt of message
    message.ack()

# start subscriber and wait for incoming messages
subscriber.subscribe(subscription_path, callback=callback)
print("Listening for messages on subscription {}. Press Ctrl+C to exit.".format(subscription_path))
while True:
    try:
        time.sleep(5)
    except KeyboardInterrupt:
        print("Exiting...")
        break
