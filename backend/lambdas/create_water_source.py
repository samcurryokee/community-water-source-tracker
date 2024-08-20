import json
import boto3
import os
from datetime import datetime
from botocore.exceptions import ClientError

# Set up DynamoDB resource
dynamodb = boto3.resource(
    'dynamodb', region_name=os.environ.get('AWS_REGION', 'us-east-1'))
table_name = os.environ['DYNAMODB_TABLE']
table = dynamodb.Table(table_name)


def lambda_handler(event, context):
    try:
        # Parse and validate input
        body = json.loads(event['body'])
        required_fields = ['SourceID', 'Location',
                           'SourceType', 'Status', 'LastReportedBy', 'Comments']
        if not all(field in body for field in required_fields):
            raise ValueError("Missing required fields")

        # Prepare item for DynamoDB
        timestamp = int(datetime.utcnow().timestamp())
        item = {
            'SourceID': body['SourceID'],
            'Timestamp': timestamp,
            'Location': body['Location'],
            'SourceType': body['SourceType'],
            'Status': body['Status'],
            'LastReportedBy': body['LastReportedBy'],
            'Comments': body['Comments'],
            'PhotoURL': body.get('PhotoURL'),
            'GPSCoordinates': body.get('GPSCoordinates')
        }

        # Put item in DynamoDB
        table.put_item(Item=item)

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Water source added successfully!'})
        }

    except ValueError as ve:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': str(ve)})
        }
    except ClientError as ce:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"DynamoDB error: {str(ce)}"})
        }
    except json.JSONDecodeError:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Invalid JSON in request body'})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"Unexpected error: {str(e)}"})
        }
