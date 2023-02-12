import functions_framework
from google.cloud import storage
from google.cloud import pubsub_v1


# main function that triggers the process
@functions_framework.http
def mainMapReduce(request):
    initialBucket = "books-inputdata"
    storage_client = storage.Client()

    # getting books from the bucket containg the original .txt files
    blobs = storage_client.list_blobs(initialBucket)

    for blob in blobs:
        print("Opening " + blob.name)

        # open each file and read the string using different encoding
        try:
            with blob.open("r", encoding='utf-8') as f:
                content = f.read()
        except:
            with blob.open("r", encoding='iso-8859-1') as f:
                content = f.read()

        print("Done preparing " + blob.name)
        publishPreppedString(content)

    return ("Map Reduce has completed and output has been produced. Output will appear here: https://console.cloud.google.com/storage/browser/connectedbooks-output")


# function that handles the publishing of the message to the mapper topic
def publishPreppedString(preppedString):
    # project specific instances
    projectId = "cloud-computing-cw-369814"
    topicId = "mappedBooks"

    publisher = pubsub_v1.PublisherClient()
    topicPath = publisher.topic_path(projectId, topicId)

    # send the book as a string message to the mapper topic
    publisher.publish(topicPath, preppedString.encode("utf-8"))
