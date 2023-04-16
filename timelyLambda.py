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
    
    #Create a boto3 client for DynamoDB & sns
    dynamodb = boto3.client('dynamodb')
    sns = boto3.client('sns')
    
    response = dynamodb.query(
        TableName = 'Courses',
        IndexName = 'hasSubscribers-index',
        KeyConditionExpression = 'hasSubscribers = :value',
        ExpressionAttributeValues={
        ':value' : {'S': 'TRUE'}
        }
    )
    #Updates course status in database & sends notification to subscribed users
    for obj in response['Items']:
        dbStatus = obj['status']['S']
        apiStatus = getAPI(obj['subjectCode']['S'], obj['courseId']['S'])
        if(apiStatus > dbStatus or apiStatus < dbStatus):
            dynamodb.update_item(
                TableName='Courses',
                Key={'courseId': {'S': obj['courseId']['S']}},
                UpdateExpression='SET #s = :new_status',
                ExpressionAttributeNames={'#s': 'status'},
                ExpressionAttributeValues={':new_status': {'S': apiStatus}}
            )
            arn = dynamodb.get_item(
                TableName='topics',
                Key={'topicName': {'S' : obj['courseId']['S']}},
                ProjectionExpression = 'ARN'
                )
            res = sns.publish(
                TopicArn = arn['Item']['ARN']['S'],#matching_topics[0]['TopicArn'],
                Message = 'The status of the course {} has changed from {} to {}'.format(obj['courseName']['S'],dbStatus,apiStatus),
                Subject = 'Course Status Change!'
            )

