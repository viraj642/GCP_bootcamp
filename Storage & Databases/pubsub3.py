from google.cloud import pubsub_v1
from google.oauth2 import service_account
import sys

if len(sys.argv)>1:
    credentials_path=sys.argv[1]
    topic_id=sys.argv[2]
else:
    credentials_path = "keys/pubsubSA_key.json"
    topic_id = "topic1"

# set up credentials
creds = service_account.Credentials.from_service_account_file(credentials_path)

# set variables project name
project_id = "viraj-patil-bootcamp"

publisher = pubsub_v1.PublisherClient(credentials=creds)
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
topic_path = publisher.topic_path(project_id, topic_id)

names = ["xxx","yyy","ccc","zzz","bbb"]

for n in range(6, 11):
    data_str = '{{"id":{}, "name":"{}"}}'.format(n,names[n-6])
    # Data must be a bytestring
    data = data_str.encode("utf-8")
    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, data)
    print(future.result())

print(f"Published messages to {topic_path}.")
