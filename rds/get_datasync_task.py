import boto3

client = boto3.client('datasync')

response = client.list_tasks()
print(response)