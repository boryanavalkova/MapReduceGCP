# Project Title

Serverless solution on GCP for the MapReduce problem.

## Description

This project is the coursework and code deliverable for 22COC105. It contains the sorce code running on GCP with comments explaining the logic of the project with the expected parameters and outputs. Please refer to the report alongside for more detailed explanation of the architecture and the workflow.

## Getting Started

### Dependencies

Each folder with code has requirements.txt listing all the required libraries for the project.
The mapping cloud function contains also a stop-words.txt used to remove the list of stop words. It is included locally in the project as a txt file.
* gen 2 cloud functions
* python 3.10
* python uuid
* python hashlib
* google cloud dependencies

* Input bucket details:
```
gs://books-inputdata
```
```
https://console.cloud.google.com/storage/browser/books-inputdata
```

* Output bucket details:
```
gs://connectedbooks-output
```
```
https://console.cloud.google.com/storage/browser/connectedbooks-output
```
### Executing program

* How to run the program
* Open the following URL in your browser https://function-mapreduce-bozoqfoicq-uc.a.run.app and wait for an output message to appear as a simple text on a blank html page. After that check the produced output in connectedbooks-output bucket https://console.cloud.google.com/storage/browser/connectedbooks-output The program should complete within a minute and output should be made available shortly afterwards (finalOutput.txt).
* [trigger-option1](https://function-mapreduce-bozoqfoicq-uc.a.run.app)
* [connectedbooks-output](https://function-mapreduce-bozoqfoicq-uc.a.run.app)

## Authors

Boryana Valkova

## Acknowledgments

Inspiration, code snippets, etc.
* [simple-readme](https://gist.github.com/DomPizzie/7a5ff55ffa9081f2de27c315f5018afc)
* [hashlib-sha256](https://docs.python.org/3/library/hashlib.html)
* [uuid-uuid4](https://docs.python.org/3/library/uuid.html)
* [cloud-storage](https://cloud.google.com/appengine/docs/legacy/standard/python/googlecloudstorageclient/read-write-to-cloud-storage)
* [pub-sub](https://cloud.google.com/pubsub/docs/publish-receive-messages-console)
