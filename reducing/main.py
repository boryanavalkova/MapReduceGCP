import base64
import uuid
import functions_framework
from google.cloud import pubsub_v1
from google.cloud import storage
from itertools import groupby


# Triggered from a message on a reduce pub/sub topic.
@functions_framework.cloud_event
def Reduce_pubsub(cloud_event):
    print("Getting data to reduce")
    # decode the string of the filename message from reduce topic subcription
    fileName = base64.b64decode(
        cloud_event.data["message"]["data"]).decode("utf-8")
    # getting the folder name from the initial message
    folderName = fileName.split("/")[0] + "/"
    # get the number of input books
    initialBookCount = getInitialBookCount()
    # count the number of folders from the shuffler
    fileFolderCount = getFileFolderCount(folderName)

    # checking if the file sent from message is the same as the latest file created
    # if yes it updates the value of the latetst blob
    bucket_name = "shuffler-bucket"
    storage_client = storage.Client()
    blobs = list(storage_client.list_blobs(bucket_name, prefix=folderName))
    latestBlob = blobs[0]
    for i in range(len(blobs)):
        if blobs[i].time_created > latestBlob.time_created:
            latestBlob = blobs[i]

    # check if there is enough files in folder to start reducing
    # there needs to be the same amount of start files as folders
    # the latest message should macth the most recent file for a reducer to get triggered
    # in this way multiple recuders are prevented from running for each folder
    if (initialBookCount == fileFolderCount and latestBlob.name == fileName):
        # calling the book reduce function
        print("Starting to reduce", folderName)
        reducing(folderName)
        print("Done reducing", folderName)


# function that goes through the files in the shuffler bucket and reduces them
# takes the name of the folder it needs to reduce
def reducing(folderName):
    shuffledBook = []
    inputList = ""
    final = ''

    # get all the files from that folder from a string to a list
    bucket_name = "shuffler-bucket"
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=folderName)
    for blob in blobs:
        with blob.open("r") as f:
            content = f.read()
            inputList += content
    shuffledBook = inputList[:-1].split(';')

   # split the values into tuples so they can be grouped by the key in each paair
   # the output looks like [sortedKey: (anagram1, anagram2, anagram3)]
   # uses list comprehantion and groupby to iterate
    inputListTuples = [tuple(pair.split(',')) for pair in shuffledBook]
    for sortedWord, words in groupby(sorted(inputListTuples), lambda i: i[0]):
      # using set so all the same words are igrnored
        anagramList = set([thing[1] for thing in words])
        if (len(anagramList)) <= 1:
            continue
        anagrams = ", ".join(anagramList)
        final += sortedWord + ":  " + anagrams + '\n'
    write_read(final)


# function that takes the reduced book folder and writes it to a file in a bucket
def write_read(reducedBook):
    # project specific instances
    bucket_name = "book-output"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    # creating randomfile names for reducer output using uuid
    # uuid docs: https://docs.python.org/3/library/uuid.html
    blob_name = "{}.txt".format(str(uuid.uuid4()))
    blob = bucket.blob(blob_name)

    with blob.open("w") as f:
        f.write(reducedBook)
    print('Output file is done')


# function that counts how many books are initially in the input bucket
# retuns an int
def getInitialBookCount():
    bucket_name = "books-inputdata"
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name)
    count = 0
    for blob in blobs:
        count += 1

    return count


# counting the amount of files in folder procuded by the shuffler
# takes a folder name so it can count the specific folder instance
# returns an int
def getFileFolderCount(folderName):
    bucket_name = "shuffler-bucket"
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=folderName)
    count = 0
    for blob in blobs:
        count += 1

    return count
