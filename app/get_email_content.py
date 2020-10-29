import os
import sys
import os.path
from collections import defaultdict
from email.parser import BytesParser

import boto3

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

CONTENT_S3_BUCKET = os.environ.get(
    'CONTENT_S3_BUCKET', 'UNDEFINED'
)

def handler(event, _):
    for record in event.get('Records', []):
        s3_bucket_name = record.get('s3').get('bucket').get('name')
        document_name = record.get('s3').get('object').get('key')
        s3_object = s3.Object(s3_bucket_name, document_name)
        email_multipart_binary = s3_object.get()['Body'].read()
        attachments = find_attachments(email_multipart_binary)
        store_attachments(attachments)

    return {
        'statusCode':200
    }

def store_attachments(attachments):
    for content_disposition, part in attachments:
        data = part.get_payload(decode=True)
        s3_client.put_object(
            Body=data, 
            Bucket=CONTENT_S3_BUCKET, 
            Key=content_disposition['filename']
        )

def find_attachments(email_multipart_binary):
    message = BytesParser().parsebytes(email_multipart_binary)
    found = []
    for part in message.walk():
        if 'content-disposition' not in part:
            continue
        content_disposition = part['content-disposition'].split(';')
        content_disposition = [x.strip() for x in content_disposition]
        if content_disposition[0].lower() != 'attachment':
            continue
        parsed = {}
        for kv in content_disposition[1:]:
            key, value = kv.split('=')
            if value.startswith('"'):
                value = value.strip('"')
            elif value.startswith("'"):
                value = value.strip("'")
            parsed[key] = value
        found.append((parsed, part))
    return found
