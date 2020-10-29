import json 
import boto3

textract = boto3.client('textract')

def handler(event, _):
    results = [] 

    for record in event.get('Records'):
        message = json.loads(record.get('Sns').get('Message'))
        job_id = message.get('JobId')
        result = get_results(job_id)
        results.append(result)

    print(results)
    
    return {
        'statusCode': 200,
        'body': results
    }
    
def get_results(job_id):
    response = textract.get_document_text_detection(
        JobId=job_id
    )
    
    pages = []
    pages.append(response)

    nextToken = None
    if 'NextToken' in response:
        nextToken = response['NextToken']

    while(nextToken):
        response = textract.get_document_text_detection(
            JobId=job_id, 
            NextToken=nextToken
        )
        pages.append(response)
        print(f'Resultset page recieved: {len(pages)}')

        nextToken = None
        if 'NextToken' in response:
            nextToken = response['NextToken']

    return pages
