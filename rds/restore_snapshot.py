import boto3
from datetime import datetime

t = datetime.now()
d1 = t.strftime("%b-%d-%Y")
t1 = t.strftime('%H-%M-%S')

prd_identifier = 'prd'
target_env_identifier = 'qa1'

client = boto3.client('rds')


def describe_instance():
    print("** getting instance details")
    response = client.describe_db_instances(
        DBInstanceIdentifier=target_env_identifier,
    )
    print("** details fetched")
    return (response)


def take_snapshot():
    print("** taking production snapshot")
    client.create_db_snapshot(
        DBSnapshotIdentifier=f'{prd_identifier}-snap-{d1}-{t1}',
        DBInstanceIdentifier=prd_identifier,
        Tags=[
            {
                'Key': 'creationDate',
                'Value': f'{d1}-{t1}'
            },
            {
                'Key': 'rdsInstance',
                'Value': prd_identifier
            },
        ]
    )
    print("** snapshot taken waiting it to complete")
    client.get_waiter('db_snapshot_completed').wait(
        DBInstanceIdentifier=prd_identifier,
        DBSnapshotIdentifier=f'{prd_identifier}-snap-{d1}-{t1}',
        Filters=[
            {
                'Name': 'db-instance-id',
                'Values': [
                    prd_identifier
                ]
            },
        ],

        WaiterConfig={
            'Delay': 15,
            'MaxAttempts': 100
        }
    )
    print("** snapshot completed")


def delete_db():
    print(f"** deleting old instance for {target_env_identifier}")
    client.delete_db_instance(
        DBInstanceIdentifier=target_env_identifier,
        SkipFinalSnapshot=False,
        FinalDBSnapshotIdentifier=f'{target_env_identifier}-final-snap-{d1}-{t1}',
        DeleteAutomatedBackups=True
    )
    print(f"** waiting instance {target_env_identifier} to be deleted")
    client.get_waiter('db_instance_deleted').wait(
        DBInstanceIdentifier=target_env_identifier,
        WaiterConfig={
            'Delay': 15,
            'MaxAttempts': 100
        }
    )
    print(f"** instance {target_env_identifier} deleted",
          f"final snapshot identifier: {target_env_identifier}-final-snap-{d1}-{t1}")


def restore_snapshot(DBInstanceClass, Port, AvailabilityZone, DBSubnetGroupName, MultiAZ, PubliclyAccessible, AutoMinorVersionUpgrade, LicenseModel,
                     Engine, OptionGroupName, StorageType, VpcSecurityGroupIdsList, DBParameterGroupName, BackupTarget, NetworkType, TagList):
    print(
        f"** restoring latest production snapshot to {target_env_identifier}")
    client.restore_db_instance_from_db_snapshot(
        DBInstanceIdentifier=target_env_identifier,
        DBSnapshotIdentifier=f'{prd_identifier}-snap-{d1}-{t1}',
        DBInstanceClass=DBInstanceClass,
        Port=Port,
        AvailabilityZone=AvailabilityZone,
        DBSubnetGroupName=DBSubnetGroupName,
        MultiAZ=MultiAZ,
        PubliclyAccessible=PubliclyAccessible,
        AutoMinorVersionUpgrade=AutoMinorVersionUpgrade,
        LicenseModel=LicenseModel,
        Engine=Engine,
        OptionGroupName=OptionGroupName,
        Tags=TagList,
        StorageType=StorageType,
        VpcSecurityGroupIds=VpcSecurityGroupIdsList,
        CopyTagsToSnapshot=True,
        DBParameterGroupName=DBParameterGroupName,
        DeletionProtection=False,
        BackupTarget=BackupTarget,
        NetworkType=NetworkType
    )
    print(f"** waiting instance {target_env_identifier} to be available")
    client.get_waiter('db_instance_available').wait(
        DBInstanceIdentifier=target_env_identifier,
        WaiterConfig={
            'Delay': 15,
            'MaxAttempts': 150
        }
    )
    print(f"** instance {target_env_identifier} is available")


print('*** Process Started')
response = describe_instance()
DBInstanceClass = response['DBInstances'][0]['DBInstanceClass']
Endpoint = response['DBInstances'][0]['Endpoint']['Address']
Port = response['DBInstances'][0]['Endpoint']['Port']
AvailabilityZone = response['DBInstances'][0]['AvailabilityZone']
DBSubnetGroupName = response['DBInstances'][0]['DBSubnetGroup']['DBSubnetGroupName']
MultiAZ = response['DBInstances'][0]['MultiAZ']
PubliclyAccessible = response['DBInstances'][0]['PubliclyAccessible']
AutoMinorVersionUpgrade = response['DBInstances'][0]['AutoMinorVersionUpgrade']
LicenseModel = response['DBInstances'][0]['LicenseModel']
Engine = response['DBInstances'][0]['Engine']
OptionGroupName = response['DBInstances'][0]['OptionGroupMemberships'][0]['OptionGroupName']
StorageType = response['DBInstances'][0]['StorageType']
VpcSecurityGroupIds = response['DBInstances'][0]['VpcSecurityGroups']
VpcSecurityGroupIdsList = [value for i in VpcSecurityGroupIds for key,
                           value in i.items() if key == "VpcSecurityGroupId"]
DBParameterGroupName = response['DBInstances'][0]['DBParameterGroups'][0]['DBParameterGroupName']
BackupTarget = response['DBInstances'][0]['BackupTarget']
NetworkType = response['DBInstances'][0]['NetworkType']
TagList = response['DBInstances'][0]['TagList']

print('*** DBInstanceClass:', DBInstanceClass)
print('*** Port:', Port)
print('*** Endpoint:', Endpoint)
print('*** AvailabilityZone:', AvailabilityZone)
print('*** DBSubnetGroupName:', DBSubnetGroupName)
print('*** MultiAZ:', MultiAZ)
print('*** PubliclyAccessible:', PubliclyAccessible)
print('*** AutoMinorVersionUpgrade:', AutoMinorVersionUpgrade)
print('*** LicenseModel:', LicenseModel)
print('*** Engine:', Engine)
print('*** OptionGroupName:', OptionGroupName)
print('*** StorageType:', StorageType)
print('*** VpcSecurityGroupIds:', VpcSecurityGroupIds)
print('*** VpcSecurityGroupIdsList:', VpcSecurityGroupIdsList)
print('*** DBParameterGroupName:', DBParameterGroupName)
print('*** BackupTarget:', BackupTarget)
print('*** NetworkType:', NetworkType)
print('*** TagList:', TagList)

take_snapshot()
delete_db()
restore_snapshot(DBInstanceClass, Port, AvailabilityZone, DBSubnetGroupName, MultiAZ, PubliclyAccessible, AutoMinorVersionUpgrade, LicenseModel,
                 Engine, OptionGroupName, StorageType, VpcSecurityGroupIdsList, DBParameterGroupName, BackupTarget, NetworkType, TagList)
print('*** Process Finished Successfully')
