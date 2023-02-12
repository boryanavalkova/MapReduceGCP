import base64
import functions_framework
import re
from google.cloud import pubsub_v1


# Triggered from a message on the mapping pub/sub topic.
@functions_framework.cloud_event
def Mapping_pubsub(cloud_event):
    # decode message from mapper topic subcription
    # each message contains the book as a string
    book = base64.b64decode(
        cloud_event.data["message"]["data"]).decode("utf-8")

    # getting the stop words
    print("Starting to prep book")
    with open("stop-words.txt", "r", encoding='utf-8') as f:
        stopWords = f.read()

    stopWords = stopWords.split(",")

    # removing the stop words from the book string
    content = book.split()
    content = [word for word in content if word.lower() not in stopWords]
    finalContent = ' '.join(content)

    # function that returns the book string only with alphabetic chars
    finalContent = cleanBookContent(finalContent)
    print("Finished preping book")

    print("Mapping data")
    # format the message
    book = finalContent.split(";")
    # make a list that contains [(sortedWord, word); (sortedWord, word);] sturcure
    mappedBook = list(map(wordSort, book))
    mappedBookStr = ";".join(mappedBook)
    print("Finished mapping")

    print("Sending message to shuffler")
    # send the mapped book as a string message to shuffler topic
    publishMappedBook(mappedBookStr)
    print("Message sent to shuffler")

    return mappedBookStr


# function that handles the cleaning up each file
def cleanBookContent(content):
    # lowercase the content & remove all numericals in content string
    content = content.lower()
    regex = re.compile('[^a-z ]')
    content = regex.sub('', content)
    # replace whitespace with semicolon
    content = content.replace(" ", ";")

    return content


# function that sorts a word from book alphabetically and creates the tuple (sortedWord, word)
def wordSort(word):
    sortedWord = "".join(sorted(word))
    newWord = ("%s,%s" % (sortedWord, word))

    return newWord


# function that handles the publishing of the message to the shuffler topic
def publishMappedBook(mappedBookStr):
    # project specific instances
    projectId = "cloud-computing-cw-369814"
    topicId = "shuffledBooks"

    publisher = pubsub_v1.PublisherClient()
    topicPath = publisher.topic_path(projectId, topicId)

    # sends the mapped book string message to the shuffler topic
    publisher.publish(topicPath, mappedBookStr.encode("utf-8"))
