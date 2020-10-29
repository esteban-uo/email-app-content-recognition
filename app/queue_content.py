import os
import boto3

textract = boto3.client('textract')

IAM_ROLE_NAME_TEXTTRACT = os.environ.get(
    'IAM_ROLE_NAME_TEXTTRACT', 'UNDEFINED'
)
SNS_TOPIC_LAMBDA_ARN = os.environ.get(
    'SNS_TOPIC_LAMBDA_ARN', 'UNDEFINED'
)

def handler(event, _):
    for record in event.get('Records', []):
        s3_bucket_name = record.get('s3').get('bucket').get('name')
        document_name = record.get('s3').get('object').get('key')

        response = textract.start_document_text_detection(
            DocumentLocation={
                'S3Object': {
                    'Bucket': s3_bucket_name,
                    'Name': document_name
                }
            },
            NotificationChannel={
                'RoleArn': IAM_ROLE_NAME_TEXTTRACT,
                'SNSTopicArn': SNS_TOPIC_LAMBDA_ARN
            }
        )

        print(response)
