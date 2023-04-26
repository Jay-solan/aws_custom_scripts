import boto3
import time

client = boto3.client('datasync')

Task1_ARN = "ARN"
Task2_ARN = "ARN"

client.start_task_execution(TaskArn=Task1_ARN)
client.start_task_execution(TaskArn=Task2_ARN)

def get_status(arn):
    response = client.describe_task_execution(TaskExecutionArn=arn)
    status = response['Status']
    return status

status_t1 = get_status(Task1_ARN)
status_t2 = get_status(Task2_ARN)

while True:    
    print("status task1:", status_t1)
    print("status task2:", status_t2)
    if status_t1 == 'SUCCESS' and status_t2 =='SUCCESS':
        print("*** tasks succeeded")
        break
    elif status_t1 == 'ERROR' or status_t2 =='ERROR':
        print("***ERROR***")
        print("status task1:", status_t1)
        print("status task2:", status_t2)
        raise Exception('Please check the failed tasks')
    else:
        status_t1 = get_status(Task1_ARN)
        status_t2 = get_status(Task2_ARN)
        print("status task1:", status_t1)
        print("status task2:", status_t2)
        time.sleep(5)

