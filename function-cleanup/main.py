import functions_framework
from google.cloud import storage

# Triggered by a change in the connectedbooks-output bucket
@functions_framework.cloud_event
def cleanUp(cloud_event):
    bucket_name = "connectedbooks-output"
    storage_client = storage.Client()
    blobs = list(storage_client.list_blobs(bucket_name))

    # check if there is already a file there to prevent mutple instances from running
    if len(blobs) == 1:
        clearInterimOutputBucket("shuffler-bucket")
        clearInterimOutputBucket("book-output")


# this function cleans up the files in the interim shuffler and recuder buckets
def clearInterimOutputBucket(bucketName):
    bucket_name = bucketName
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = storage_client.list_blobs(bucket_name)
    for blob in blobs:
        blob_name = blob.name
        blob = bucket.blob(blob_name)
        print(bucketName, blob_name)
        blob.delete()

    print("Deleted files in ", bucket_name)
