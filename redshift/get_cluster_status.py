import logging
import boto3
import time

def get_cluster_status(redshift_client, cluster_name):
    """
    Gets the current status of the specified Redshift cluster.

    Args:
        redshift_client (boto3.client): The Redshift client.
        cluster_name (str): The name of the Redshift cluster.

    Returns:
        str: The current status of the cluster.
    """
    response = redshift_client.describe_clusters(ClusterIdentifier=cluster_name)
    
    return response['Clusters'][0]['ClusterStatus']

session = boto3.Session(profile_name="DEV", region_name='ap-south-1')
redshift_client = session.client('redshift')

cluster_name = "redshift-cluster-1"

while True:
    time.sleep(1)
    status = get_cluster_status(redshift_client, cluster_name)
    print(status)