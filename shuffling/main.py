import base64
import functions_framework
import hashlib
import uuid
from google.cloud import storage
from google.cloud import pubsub_v1


# Triggered from a message sent to the shuffler pub/sub topic.
@functions_framework.cloud_event
def Shuffling_pubsub(cloud_event):
    print("Getting data to shuffle")
    # decode message from shuffler topic subcription
    # each message contains the mapped book as a string
    mappedBook = base64.b64decode(
        cloud_event.data["message"]["data"]).decode("utf-8")

    # format the message
    mappedBook = mappedBook.split(";")

    # shuffle and send to reducer
    splitShuffledBooks(mappedBook)
    print("Message sent to reducer")


# function that hashes each sorted key from the word and writes the values to a file
def splitShuffledBooks(mappedBook):
   # dictionary that holds all the values
    splitShuffled = {}
    # number of reducers
    reducerNum = 16
    # assign a key number id for each hashed key
    for number in range(reducerNum):
        splitShuffled[number] = ''

    # hashing object instance using the hashlib library
    # loops through the mapped string and hashes the key to create the structured dictionary
    # haslib docs: https://docs.python.org/3/library/hashlib.html
    for mappedPair in mappedBook:
        hashObj = hashlib.sha256((mappedPair.split(",")[0]).encode("utf-8"))
        hashKey = hashObj.hexdigest()
        splitShuffled[int(hashKey, 16) % reducerNum] += mappedPair + ";"
    writeShuffleData(splitShuffled)
    print("Sending message to reducer")

    return("Finished shuffling")


# function that handles the publishing of the message to the reducer topic
def publishShuffledBook(fileName):
    # project specific instances
    projectId = "cloud-computing-cw-369814"
    topicId = "reducedBooks"

    publisher = pubsub_v1.PublisherClient()
    topicPath = publisher.topic_path(projectId, topicId)

    # send to the reducer topic message with the filename of each completed hashed set
    publisher.publish(topicPath, fileName.encode("utf-8"))


def writeShuffleData(splitShuffled):
    # project specific instances
    bucket_name = "shuffler-bucket"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # creation of dynamic files that match the folder structure based on the key id
    for key, value in splitShuffled.items():
        # using uuid to create a random value used for file name
        blob_name = "folder{}/{}.txt".format(key, str(uuid.uuid4()))
        blob = bucket.blob(blob_name)
        with blob.open("w") as f:
            f.write(value)
        publishShuffledBook(blob_name)
