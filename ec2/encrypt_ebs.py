'''
: Stop instance Done
: Wait for instance to stop Done
: Create snapshot of volume Done
: Detach existing volume
: Launch new encripted volume with the snapshot
: Attach new volume
: Start instance back

'''
import csv
import boto3
import pandas as pd
from datetime import datetime

t = datetime.now()
d1 = t.strftime("%b-%d-%Y")
t1 = t.strftime('%H-%M-%S')


client = boto3.client('ec2')
ec2 = boto3.resource('ec2')


def stop_instance(instance_id):
    print(f'Stopping instance {instance_id}...')
    client = boto3.client('ec2')
    client.stop_instances(InstanceIds=[instance_id])
    client.get_waiter('instance_stopped').wait(InstanceIds=[instance_id])
    print(f'*** Instance {instance_id} has been stopped')

# stop_instance()


def create_snapshot(volume_id):
    print(f'Taking snapshot for volume id: {volume_id} ...')
    volume = ec2.Volume(volume_id)

    snapshot = volume.create_snapshot(
        Description=f'Snapshot taken for {volume_id} before encryption',
        TagSpecifications=[
            {
                'ResourceType': 'snapshot',
                'Tags': [
                    {
                        'Key': 'VolumeId',
                        'Value': volume_id
                    },
                    {
                        'Key': 'Name',
                        'Value': f'snap-unencr'
                    }
                ]
            }
        ]
    )
    # print(snapshot)
    # print(snapshot.id)

    client.get_waiter('snapshot_completed').wait(SnapshotIds=[snapshot.id], WaiterConfig={
        'Delay': 20,
        'MaxAttempts': 100
    })
    print(f'*** Snapshot taken for {volume_id}, snapshotId: {snapshot.id}')

    return (snapshot)


def detach_root_vol(volume_id, instance_id):
    print(f'Detaching volume {volume_id} from {instance_id}...')
    volume = ec2.Volume(volume_id)
    response = volume.detach_from_instance(
        Device='/dev/xvda',
        Force=True,
        InstanceId=instance_id,
        DryRun=False
    )
    print(f'Volume {volume_id} detached from {instance_id}')


def create_volume(snapshot_id, AZ):
    print(f'Creating new volume from {snapshot_id} in {AZ}...')
    response = client.create_volume(
        AvailabilityZone=AZ,
        Encrypted=True,
        SnapshotId=snapshot_id,
        VolumeType='gp2',
        TagSpecifications=[
            {
                'ResourceType': 'volume',
                'Tags': [
                    {
                        'Key': 'Name',
                        'Value': 'vol-encrypted'
                    },
                    {
                        'Key': 'SnapshotId',
                        'Value': snapshot_id
                    },
                ]
            },
        ],

    )
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        new_volume_id = response['VolumeId']
        print('New volume id: ', new_volume_id)

        client.get_waiter('volume_available').wait(
            VolumeIds=[new_volume_id],

        )
        print('***Success!! volume:', new_volume_id, 'created...')

        return (new_volume_id)


def attach_volume(instance_id, new_volume_id):
    print(f'Attaching new volume {new_volume_id} to {instance_id}...')
    response = client.attach_volume(
        Device='/dev/xvda',
        InstanceId=instance_id,
        VolumeId=new_volume_id
    )

    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        client.get_waiter('volume_in_use').wait(
            VolumeIds=[new_volume_id],
            DryRun=False
        )
        print('***Success!! volume:', new_volume_id,
              'is attached to instance:', instance_id)


def start_instance(instance_id):
    print(f'Starting instance {instance_id}...')
    response = client.start_instances(
        InstanceIds=[
            instance_id
        ]
    )
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        client.get_waiter('instance_running').wait(
            InstanceIds=[instance_id],
            DryRun=False
        )
        print(
            f'***Success!! Instance {instance_id} is running with new encrypted volume')


df = pd.read_csv('input.csv')

data = []
columns = ['instance_id', 'root_volume_id', 'size',
           'AvailabilityZone', 'new_volume_id', 'snapshot_id']

for i, row in df.iterrows():
    print(i)
    instance_id = row['instance_id']
    volume_id = row['root_volume_id']
    availability_zone = row['AvailabilityZone']
    instance_state = row['instance_state']
    print(instance_id, volume_id, availability_zone)

    print(f'Starting operation for {instance_id}')
    stop_instance(instance_id)
    snapshot_id = create_snapshot(volume_id).id
    detach_root_vol(volume_id, instance_id)
    new_volume_id = create_volume(snapshot_id, availability_zone)
    attach_volume(instance_id, new_volume_id)
    if instance_state == 'running':
        start_instance(instance_id)
    print(f'Operation successful for {instance_id}')

    new_row = [instance_id, volume_id,
               availability_zone, new_volume_id, snapshot_id]

    data.append(new_row)

    with open(f'output_after_encryption_{d1}_{t1}.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(columns)
        csvwriter.writerows(data)
