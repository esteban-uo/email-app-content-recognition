import os
import subprocess
from aws_cdk import (
    core,
    aws_lambda,
    aws_s3,
    aws_s3_notifications,
    aws_iam,
    aws_sns,
    aws_sns_subscriptions,
)

EMAILS_S3_BUCKET = os.environ.get(
    'EMAILS_S3_BUCKET', 'emails-bucket'
)
CONTENT_S3_BUCKET = os.environ.get(
    'CONTENT_S3_BUCKET', 'content-bucket'
)

class Stack(core.Stack):
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        content_s3_bucket = self.create_content_resources()
        self.create_get_content_resources(content_s3_bucket)

    def create_get_content_resources(self, content_s3_bucket):
        textract_all_policy = self.create_textextract_all_policy()
        lambda_queue_content_role = self.create_lambda_queue_content_role(
            textract_all_policy, 
            content_s3_bucket
        )

        textract_sns_topic = self.create_textract_sns_topic()
        textract_role = self.create_textract_role()   
        lambda_queue_content = self.create_lambda_queue_content(
            lambda_queue_content_role, 
            textract_role,
            textract_sns_topic
        )
        s3_content_notification = aws_s3_notifications.LambdaDestination(lambda_queue_content)
        content_s3_bucket.add_event_notification(
            aws_s3.EventType.OBJECT_CREATED_PUT, 
            s3_content_notification
        )

        lambda_get_content_role = self.create_lambda_get_content_role(textract_all_policy)
        lambda_get_content = self.create_lambda_get_content(lambda_get_content_role)
        textract_sns_topic.add_subscription(aws_sns_subscriptions.LambdaSubscription(lambda_get_content))
        textract_sns_topic.add_to_resource_policy(aws_iam.PolicyStatement(
            effect=aws_iam.Effect.ALLOW,
            actions=[
                'SNS:GetTopicAttributes',
                'SNS:SetTopicAttributes',
                'SNS:AddPermission',
                'SNS:RemovePermission',
                'SNS:DeleteTopic',
                'SNS:Subscribe',
                'SNS:ListSubscriptionsByTopic',
                'SNS:Publish',
                'SNS:Receive',
            ],
            principals=[aws_iam.ArnPrincipal('*')],
            resources=[textract_sns_topic.topic_arn]
        ))

    def create_content_resources(self):
        emails_s3_bucket = self.create_emails_s3_bucket()
        content_s3_bucket = self.create_content_s3_bucket()
        lambda_get_email_content_role = self.create_lambda_get_email_content_role(
            emails_s3_bucket,
            content_s3_bucket
        )
        lambda_get_email_content = self.create_lambda_get_email_content(
            lambda_get_email_content_role,
            emails_s3_bucket,
            content_s3_bucket
        )
        s3_get_email_content_notification = aws_s3_notifications.LambdaDestination(lambda_get_email_content)
        emails_s3_bucket.add_event_notification(
            aws_s3.EventType.OBJECT_CREATED_PUT, 
            s3_get_email_content_notification
        )
        return content_s3_bucket

    def create_textract_sns_topic(self):
        textract_sns_topic = aws_sns.Topic(self,'topic-amazontextractservice')
        return textract_sns_topic

    def create_textextract_all_policy(self):
        text_extract_all_policy = aws_iam.PolicyStatement(
            effect=aws_iam.Effect.ALLOW,
            actions=['textract:*'],
            resources=['*'],
        )
        return text_extract_all_policy

    def create_lambda_get_content(self, lambda2_role):
        lambda_function = aws_lambda.Function(
            self, 
            'get_content',
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            code=aws_lambda.Code.asset(f'../app'),
            handler='get_content.handler',
            role=lambda2_role
        )
        return lambda_function

    def create_lambda_queue_content(self, lambda_role, textract_role, textract_sns_topic):
        lambda_function = aws_lambda.Function(
            self, 
            'queue_content',
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            code=aws_lambda.Code.asset(f'../app'),
            handler='queue_content.handler',
            role=lambda_role
        )
        lambda_function.add_environment('IAM_ROLE_NAME_TEXTTRACT', textract_role.role_arn)
        lambda_function.add_environment('SNS_TOPIC_LAMBDA_ARN', textract_sns_topic.topic_arn)

        return lambda_function

    def create_lambda_get_email_content(self, lambda_role, emails_s3_bucket, content_s3_bucket):
        lambda_function = aws_lambda.Function(
            self, 
            'get_email_content',
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            code=aws_lambda.Code.asset(f'../app'),
            handler='get_email_content.handler',
            role=lambda_role
        )
        lambda_function.add_environment('CONTENT_S3_BUCKET', content_s3_bucket.bucket_name)

        return lambda_function

    def create_lambda_get_email_content_role(self, emails_s3_bucket, content_s3_bucket):
        lambda_document = aws_iam.PolicyDocument()
        lambda_document.add_statements(
            aws_iam.PolicyStatement(
                effect=aws_iam.Effect.ALLOW,
                actions=['s3:*'],
                resources=[f'{emails_s3_bucket.bucket_arn}/*'],
            )
        )
        lambda_document.add_statements(
            aws_iam.PolicyStatement(
                effect=aws_iam.Effect.ALLOW,
                actions=['s3:*'],
                resources=[f'{content_s3_bucket.bucket_arn}/*'],
            )
        )
        lambda_role = aws_iam.Role(self,
            'lambda-getemailcontentrole',
            assumed_by=aws_iam.ServicePrincipal('lambda.amazonaws.com'),
            inline_policies=[lambda_document],
        )
        lambda_role.add_managed_policy(
            aws_iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole')
        )
        return lambda_role

    def create_textract_role(self):
        text_extract_document = aws_iam.PolicyDocument()
        text_extract_document.add_statements(
            aws_iam.PolicyStatement(
                effect=aws_iam.Effect.ALLOW,
                actions=['sns:Publish'],
                resources=['arn:aws:sns:*:*:AmazonTextract*'],
            )
        )
        textract_role = aws_iam.Role(self,
            'role-amazontextractservicerole',
            assumed_by=aws_iam.ServicePrincipal('textract.amazonaws.com'),
            inline_policies=[text_extract_document],
        )
        return textract_role

    def create_lambda_get_content_role(self, text_extract_all_policy):
        policy_document = aws_iam.PolicyDocument()
        policy_document.add_statements(text_extract_all_policy)
        lambda_role = aws_iam.Role(self,
            'lambda-processrole',
            assumed_by=aws_iam.ServicePrincipal('lambda.amazonaws.com'),
            inline_policies=[policy_document],
        )
        lambda_role.add_managed_policy(
            aws_iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole')
        )
        return lambda_role

    def create_lambda_queue_content_role(self, text_extract_all_policy, content_s3_bucket):
        lambda_document = aws_iam.PolicyDocument()
        lambda_document.add_statements(text_extract_all_policy)
        lambda_document.add_statements(
            aws_iam.PolicyStatement(
                effect=aws_iam.Effect.ALLOW,
                actions=['s3:*'],
                resources=[f'{content_s3_bucket.bucket_arn}/*'],
            )
        )
        lambda_role = aws_iam.Role(self,
            'lambda-queuecontentrole',
            assumed_by=aws_iam.ServicePrincipal('lambda.amazonaws.com'),
            inline_policies=[lambda_document],
        )
        lambda_role.add_managed_policy(
            aws_iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole')
        )
        return lambda_role


    def create_content_s3_bucket(self):
        return aws_s3.Bucket(self, CONTENT_S3_BUCKET)

           
    def create_emails_s3_bucket(self):
        return aws_s3.Bucket(self, EMAILS_S3_BUCKET)
