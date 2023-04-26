import boto3
import csv
from datetime import datetime

t = datetime.now()
d1 = t.strftime("%b-%d-%Y")
t1 = t.strftime('%H-%M-%S')

client = boto3.client('ec2')

instance_state = "running" # pending | running | shutting-down | terminated | stopping | stopped
tag_key = 'Name'  # could be any tag key
tag_values = ['FSS']  # could be any tag value
device = '/dev/xvda' # put device name


data = []
columns = ['instance_id', 'root_volume_id', 'size',
           'AvailabilityZone', 'instance_state']


def get_instances():

    response = client.describe_instances(Filters=[
        {
            'Name': 'instance-state-name',
            'Values': [
                instance_state
            ]
        },
        {
            'Name': f'tag:{tag_key}',
            'Values': tag_values

        }
    ])
    reservations = response['Reservations']
    for i in reservations:
        for j in i['Instances']:
            instance_id = (j['InstanceId'])
            response_vol = client.describe_volumes(
                Filters=[
                    {
                        'Name': 'attachment.instance-id',
                        'Values': [instance_id]

                    },
                    {
                        'Name': 'attachment.device',
                        'Values': [device]

                    },
                    {
                        'Name': 'encrypted',
                        'Values': ['false']

                    }
                ]
            )

            for i in response_vol['Volumes']:
                root_vol = i["VolumeId"]
                size = i['Size']
                az = i['AvailabilityZone']

                row = [instance_id, root_vol, size, az, instance_state]
                print(row)
                data.append(row)

    with open(f'output_{d1}_{t1}.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(columns)
        csvwriter.writerows(data)


get_instances()
