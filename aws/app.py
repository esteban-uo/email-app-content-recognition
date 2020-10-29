import os
from aws_cdk import core

from stack import Stack

ENV = dict(
    account=os.environ.get('AWS_ACCOUNT_ID'),
    region=os.environ.get('AWS_DEFAULT_REGION')
)

app = core.App()

Stack(app, 'tsi-appcontentrecognition', env=ENV)

app.synth()
