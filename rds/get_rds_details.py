import boto3
target_env_identifier = 'qa1'

client = boto3.client('rds')


def describe_instance():
    print("** getting instance details")
    response = client.describe_db_instances(
        DBInstanceIdentifier=target_env_identifier,
    )
    print("** details fetched")
    return (response)

response = describe_instance()
print(response)