import json
import boto3
import requests


def lambda_handler(event, context):
    def getAPI(subjectCode,courseID):
      currStatus = ''
      term = "1234"
      response = requests.get("https://public.enroll.wisc.edu/api/search/v1/enrollmentPackages/{}/{}/{}".format(term,subjectCode,courseID))
      response_json = json.loads(response.text)
      for obj in response_json:
          currStatus += obj['packageEnrollmentStatus']['status'] 
      if('OPEN' in currStatus):
          return 'OPEN'
      elif('WAITLISTED' in currStatus):
          return 'WAITLISTED'
      return 'CLOSED'
      
    #Create a boto3 client for DynamoDB
    dynamodb = boto3.client('dynamodb')
    
    #Query courses with subscribers
    response = dynamodb.query(
        TableName = 'coursesWithStatuses',
        IndexName = 'hasSubscribers-index',
        KeyConditionExpression = 'hasSubscribers = :value',
        ExpressionAttributeValues={
        ':value' : {'S': 'TRUE'}
        }
    )
    
    #Compare statuses of courses in API vs dynamodb table
    for obj in response['Items']:
        dbStatus = obj['status']['S']
        apiStatus = getAPI(obj['subjectCode']['S'], obj['courseId']['S'])
        if(apiStatus > dbStatus or apiStatus < dbStatus):
            dynamodb.update_item(
                TableName='coursesWithStatuses',
                Key={'courseId': {'S': obj['courseId']['S']}},
                UpdateExpression='SET #s = :new_status',
                ExpressionAttributeNames={'#s': 'status'},
                ExpressionAttributeValues={':new_status': {'S': apiStatus}}
            )
        








  #{'Items': [{'numSubscribers': {'N': '0'}, 'subscribers': {'L': [{'S': 'adi4pillai@gmail.com'}]}, 'status': {'S': 'CLOSED'}, 'courseId': {'S': '024798'}, 'hasSubscribers': {'S': 'TRUE'}, 'subjectCode': {'S': '266'}, 'courseName': {'S': 'COMP SCI 400'}}, {'numSubscribers': {'N': '0'}, 'subscribers': {'L': []}, 'status': {'S': 'WAITLISTED'}, 'courseId': {'S': '012904'}, 'hasSubscribers': {'S': 'TRUE'}, 'subjectCode': {'S': '660'}, 'courseName': {'S': 'MUSIC 60'}}], 'Count': 2, 'ScannedCount': 2, 'ResponseMetadata': {'RequestId': 'O93BEJP5N3JEOP3MOPBCPDVJVNVV4KQNSO5AEMVJF66Q9ASUAAJG', 'HTTPStatusCode': 200, 'HTTPHeaders': {'server': 'Server', 'date': 'Mon, 10 Apr 2023 17:05:54 GMT', 'content-type': 'application/x-amz-json-1.0', 'content-length': '450', 'connection': 'keep-alive', 'x-amzn-requestid': 'O93BEJP5N3JEOP3MOPBCPDVJVNVV4KQNSO5AEMVJF66Q9ASUAAJG', 'x-amz-crc32': '2576712362'}, 'RetryAttempts': 0}}



 # {'Items': [{'numSubscribers': {'N': '0'}, 'subscribers': {'L': [{'S': 'adi4pillai@gmail.com'}]}, 'status': {'S': 'CLOSED'}, 'courseId': {'S': '024798'}, 'hasSubscribers': {'S': 'TRUE'}, 'subjectCode': {'S': '266'}, 'courseName': {'S': 'COMP SCI 400'}}, {'numSubscribers': {'N': '0'}, 'subscribers': {'L': []}, 'status': {'S': 'WAITLISTED'}, 'courseId': {'S': '012904'}, 'hasSubscribers': {'S': 'TRUE'}, 'subjectCode': {'S': '660'}, 'courseName': {'S': 'MUSIC 60'}}], 'Count': 2, 'ScannedCount': 2, 'ResponseMetadata': {'RequestId': 'O93BEJP5N3JEOP3MOPBCPDVJVNVV4KQNSO5AEMVJF66Q9ASUAAJG', 'HTTPStatusCode': 200, 'HTTPHeaders': {'server': 'Server', 'date': 'Mon, 10 Apr 2023 17:05:54 GMT', 'content-type': 'application/x-amz-json-1.0', 'content-length': '450', 'connection': 'keep-alive', 'x-amzn-requestid': 'O93BEJP5N3JEOP3MOPBCPDVJVNVV4KQNSO5AEMVJF66Q9ASUAAJG', 'x-amz-crc32': '2576712362'}, 'RetryAttempts': 0}}

    
    
    # s  = {
    # "TableName": "courses",
    # "KeyConditionExpression": "hasSubscribers = :a",
    # "ExpressionAttributeValues": {
    #     ":a": True,
    # }
    # }   

    # for obj in s:
    #      response = dynamodb.update_item(
    #      TableName='courses',
    #      Key={'courseId': {'S': obj[0]}},
    #      UpdateExpression='SET #s = :new_subscribers',
    #      ExpressionAttributeNames={'#s': 'hasSubscribers'},
    #      ExpressionAttributeValues={':new_subscribers': {'BOOL': False}}
    #     )
  
  # Example item to be added to the table
    # item = {
    #   "courseId": {
    #     "S": "COMP SCI 400"
    #   },
    #   "status": {
    #     "S": "CLOSED"
    #   },
    #   "subscriberCount": {
    #     "N": "15"
    #   }
    # }
  
  # #Create a boto3 client for DynamoDB
  #  dynamodb = boto3.client('dynamodb')
  #   term = "1234"
    
    
    

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    # Put the item into the table
    # response = dynamodb.put_item(
    #     TableName='Tester1',
    #     Item=item
    # )
    
    
    
    
    
    
    
    
    
    
    
    
    
    # Update the item with a new status value
    # response = dynamodb.update_item(
    #     TableName='Tester1',
    #     Key={'courseId': {'S': 'ENG 100'}},
    #     UpdateExpression='SET #s = :new_status',
    #     ExpressionAttributeNames={'#s': 'status'},
    #     ExpressionAttributeValues={':new_status': {'S': 'WAITLISTED'}}
    # )
    
    
    
    
    
    
    
    
    # return {
    #     'statusCode': 200,
    #     'body': 'Item updated successfully'
    # }
   
   
   
