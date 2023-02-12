import functions_framework
from google.cloud import storage

# Triggered by a change in the book-output bucket
@functions_framework.cloud_event
def combining_books(cloud_event):
    # gets the latest folder number
    folderCount = getFolderCount()
    # count the files produced by the recer in the books-output bucket
    outputFileCount = getReducedFilesCount()

    bucket_name = "connectedbooks-output"
    storage_client = storage.Client()
    blobs = list(storage_client.list_blobs(bucket_name))

    # check if the amount of folders is the same as the reduced files
    # because there are as many files as reducers and folders
    # check if there isn't connected output already to prevent multiple instances from running
    if (folderCount == outputFileCount and len(blobs) <= 0):
        print("Connecting files")
        connectOutputFiles()
    else:
        print("Wrong count")


# function that goes through the book-output bucket
# writes the connected file as a single txt
def connectOutputFiles():
    print("connecting files")
    bucket_name = "book-output"
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name)
    connectedString = ""
    for blob in blobs:
        with blob.open("r") as f:
            content = f.read()
            connectedString += content + '\n'

    writeConnectedString(connectedString[:-1])


# function that checks the latest created folder number
# ensuring the reducer has finished
# returns an int the most recent file number
def getFolderCount():
    bucket_name = "shuffler-bucket"
    storage_client = storage.Client()
    blobs = list(storage_client.list_blobs(bucket_name))
    lastBlob = blobs[0]
    for blob in blobs:
        if int(blob.name.split('/')[0].replace("folder", "")) > int(lastBlob.name.split('/')[0].replace("folder", "")):
            lastBlob = blob
    lastBlobNum = int(lastBlob.name.split('/')[0].replace("folder", ""))
    lastBlobNum = lastBlobNum + 1

    return lastBlobNum


# function that counts the number of files produced by the reducer
# returns an int the count
def getReducedFilesCount():
    bucket_name = "book-output"
    storage_client = storage.Client()
    blobs = list(storage_client.list_blobs(bucket_name))
    count = 0
    for blob in blobs:
        count += 1

    return count


# function that writes to the connectedbooks-output bucket
# creates a finalOutput.txt file containing
def writeConnectedString(connectedString):
    # project specific instances
    bucket_name = "connectedbooks-output"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob_name = "finalOutput.txt"
    blob = bucket.blob(blob_name)

    with blob.open("w") as f:
        f.write(connectedString)
    print('Output files are connected.')
