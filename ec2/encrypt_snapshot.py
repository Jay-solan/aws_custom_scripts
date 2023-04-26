import boto3

region = 'ap-south-1'  # change it
kms_key_id = ''  # Puth KMS key in quotes

ec2 = boto3.resource('ec2')

client = boto3.client('ec2')

snap_list = client.describe_snapshots(
    OwnerIds=['self'],
    Filters=[
        {
            'Name': 'encrypted',
            'Values': [
                'false'
            ]
        },
    ],
)

snapshot_list = snap_list['Snapshots']

for i in snapshot_list:
    snap_id = i['SnapshotId']
    vol_id = i["VolumeId"]
    print('Copying Snapshot for :', snap_id)
    snapshot = ec2.Snapshot(snap_id)
    response = snapshot.copy(
        Description=f'Encrypted snapshot for {snap_id}',
        Encrypted=True,
        KmsKeyId=kms_key_id,
        SourceRegion=region,
        TagSpecifications=[
            {
                'ResourceType': 'snapshot',
                'Tags': [
                    {
                        'Key': 'Name',
                        'Value': f'snap-encrypted-{snap_id}'
                    },
                    {
                        'Key': 'VolumeId',
                        'Value': vol_id
                    }
                ]
            },
        ]
    )
    print(f"Waiting for snapshot to complete: {response['SnapshotId']}")
    client.get_waiter('snapshot_completed').wait(SnapshotIds=[response["SnapshotId"]], WaiterConfig={
        'Delay': 20,
        'MaxAttempts': 100
    })
    print(
        f'*** Snapshot encrypted for {snap_id} new snapshotId: {snapshot.id}')
