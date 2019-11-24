import boto3
import time

def startJob(s3BucketName, objectName):
    response = None
    client = boto3.client('textract')
    response = client.start_document_text_detection(
    DocumentLocation={
        'S3Object': {
            'Bucket': s3BucketName,
            'Name': objectName
        }
    })

    return response["JobId"]

def isJobComplete(jobId):
    time.sleep(5)
    client = boto3.client('textract')
    response = client.get_document_text_detection(JobId=jobId)
    status = response["JobStatus"]
    print("Job status: {}".format(status))

    while(status == "IN_PROGRESS"):
        time.sleep(5)
        response = client.get_document_text_detection(JobId=jobId)
        status = response["JobStatus"]
        print("Job status: {}".format(status))

    return status

def getJobResults(jobId):

    pages = []

    time.sleep(5)

    client = boto3.client('textract')
    response = client.get_document_text_detection(JobId=jobId)
    
    pages.append(response)
    print("Resultset page recieved: {}".format(len(pages)))
    nextToken = None
    if('NextToken' in response):
        nextToken = response['NextToken']

    while(nextToken):
        time.sleep(5)

        response = client.get_document_text_detection(JobId=jobId, NextToken=nextToken)

        pages.append(response)
        print("Resultset page recieved: {}".format(len(pages)))
        nextToken = None
        if('NextToken' in response):
            nextToken = response['NextToken']

    return pages

#print(response)
def print_unformatted_response(response):
    # Print detected text
    for resultPage in response:
        for item in resultPage["Blocks"]:
            if item["BlockType"] == "LINE":
                #print ('\033[94m' +  item["Text"] + '\033[0m')
                print (item["Text"])
      
import json
          
def print_formatted_response(response):
    
   
    with open('output.txt', 'w') as outfile:
        json.dump(response, outfile)
    
    for resultPage in response:
        
        # Detect columns and print lines
        columns = []
        lines = []
        
        for item in resultPage["Blocks"]:
            if item["BlockType"] == "LINE":
                column_found=False
                for index, column in enumerate(columns):
                    bbox_left = item["Geometry"]["BoundingBox"]["Left"]
                    bbox_right = item["Geometry"]["BoundingBox"]["Left"] + item["Geometry"]["BoundingBox"]["Width"]
                    bbox_centre = item["Geometry"]["BoundingBox"]["Left"] + item["Geometry"]["BoundingBox"]["Width"]/2
                    column_centre = column['left'] + column['right']/2
        
                    if (bbox_centre > column['left'] and bbox_centre < column['right']) or (column_centre > bbox_left and column_centre < bbox_right):
                        #Bbox appears inside the column
                        lines.append([index, item["Text"]])
                        column_found=True
                        break
                if not column_found:
                    columns.append({'left':item["Geometry"]["BoundingBox"]["Left"], 'right':item["Geometry"]["BoundingBox"]["Left"] + item["Geometry"]["BoundingBox"]["Width"]})
                    lines.append([len(columns)-1, item["Text"]])
    
        lines.sort(key=lambda x: x[0])
        for line in lines:
            print (line[1])
        
# Document
s3BucketName = "te-docs"
documentName = "multi_column.pdf"

jobId = startJob(s3BucketName, documentName)
print("Started job with id: {}".format(jobId))
if(isJobComplete(jobId)):
    extracted_text = getJobResults(jobId)
    print_formatted_response(extracted_text)