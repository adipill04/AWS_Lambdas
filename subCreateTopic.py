import boto3
import json
    
def create_SNS_topic(name):
    try:
        client = boto3.client("sns")
        result = client.create_topic(Name = name)
        return result["TopicArn"]
    except Exception as e:
        print("Error occured: ", e)
        return None
        
def subscribe_user(topic_ARN, protocol, endpoint):
    try:
        client = boto3.client("sns")
        result = client.subscribe(TopicArn = topic_ARN, Protocol = protocol, Endpoint = endpoint)
        return True
    except Exception as e:
        print("Error occured: ", e)
        return False

def check_topic(topic_name):
    try:
        client = boto3.client("dynamodb")
        result = client.get_item(
            TableName = "topics", 
            Key = {"topicName" : {"S" : topic_name}}, 
            ProjectionExpression = "ARN"
        )
        topic_DNE = result["Items"][0]["ARN"]["NULL"]
        if topic_DNE:
            # DNE = Does Not Exist
            return "DNE"
        else:
            return result["Items"][0]["ARN"]["S"]
    except Exception as e:
        print("Error occured: ", e)
        return "DNE"

def add_topic(topic_name):
    try:
        topic_ARN = create_SNS_topic(topic_name)
        client = boto3.client("dynamodb")
        result = client.put_item(
            TableName="topics", 
            Item = {
                "topicName" : {"S" : topic_name},
                "ARN" : {"S" : topic_ARN}
            }
        )
        return topic_ARN
    except Exception as e:
        print("Error occured: ", e)
        return False

#def update_courses_DB(course_id):
#    try:
#        client = boto3.client("dynamodb")
#        result = client.put_item(
#            TableName = "Courses", 
#            Key = {"courseId" : {"S" : course_id}},
#            UpdateExpression = 
#        )
#    except Exception as e:
#        print("Error occured: ", e)
#        return False

def lambda_handler(event, context):
    # TODO implement
    topic_name = event["topic_name"]
    protocol = event["protocol"]
    endpoint = event["endpoint"]
    topic_ARN = check_topic(topic_name)
    to_return = False
    if topic_ARN == "DNE":
        topic_ARN = add_topic(topic_name)
    to_return = subscribe_user(topic_ARN, protocol, endpoint)
#    update_courses_DB(topic_name)
    return to_return
