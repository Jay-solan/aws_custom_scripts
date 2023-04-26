import logging
import boto3
import time

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()


def is_aws_profile_available(profile_name):
    """
    Checks if the specified AWS credentials profile is available in the ~/.aws/credentials file.

    Args:
        profile_name (str): The name of the AWS credentials profile to check.

    Returns:
        bool: True if the profile is available, False otherwise.
    """
    try:
        session = boto3.Session(profile_name=profile_name)
        session.client('sts').get_caller_identity()
        return True
    except Exception as e:
        logger.warning(
            f"AWS credentials profile '{profile_name}' is not available: {e}")
        return False


def check_cluster_availability(redshift_client, cluster_name):
    """
    Checks if the specified Amazon Redshift cluster is available in the environment.

    Parameters:
    redshift_client (boto3.client): Boto3 client for Amazon Redshift.
    cluster_name (str): The name of the Amazon Redshift cluster to check.

    Returns:
    bool: True if the cluster is available, False otherwise.
    """

    try:
        all_cluster_state = redshift_client.describe_clusters()['Clusters']

        if cluster_name in str(all_cluster_state):
            logger.info(f"Cluster '{cluster_name}' is present in environment")
            return True
        else:
            return False

    except Exception as e:
        logger.warning(f"Error Fetching clusters: {e}")
        return False


def get_cluster_status(redshift_client, cluster_name):
    """
    Gets the current status of the specified Redshift cluster.

    Args:
        redshift_client (boto3.client): The Redshift client.
        cluster_name (str): The name of the Redshift cluster.

    Returns:
        str: The current status of the cluster.
    """
    response = redshift_client.describe_clusters(
        ClusterIdentifier=cluster_name)

    return response['Clusters'][0]['ClusterStatus']


def wait_for_cluster_status(redshift_client, cluster_name, target_status):
    """
    Waits for the specified Redshift cluster to reach the target status.

    Args:
        redshift_client (boto3.client): The Redshift client.
        cluster_name (str): The name of the Redshift cluster.
        target_status (str): The target status to wait for.

    Returns:
        None
    """
    while True:
        cluster_status = get_cluster_status(redshift_client, cluster_name)
        if cluster_status == target_status:
            logger.info(
                f"Cluster '{cluster_name}' is now in '{target_status}' status")
            break
        else:
            logger.info(
                f"Waiting for cluster '{cluster_name}' to reach '{target_status}' status (current status: {cluster_status})...")
            time.sleep(10)


def resume_cluster(redshift_client, cluster_name):
    """
    Resumes the specified Redshift cluster.

    Args:
        redshift_client (boto3.client): The Redshift client.
        cluster_name (str): The name of the Redshift cluster.

    Returns:
        None
    """
    logger.info(f"Resuming cluster '{cluster_name}'...")

    try:
        redshift_client.resume_cluster(ClusterIdentifier=cluster_name)
    except Exception as e:
        logger.error(f"Failed to resume cluster '{cluster_name}': {e}")
        raise

    logger.info(f"Cluster '{cluster_name}' has been resumed")


def take_final_snapshot(redshift_client, cluster_name, snapshot_identifier, retention_period):
    """
    Takes a final snapshot of the specified Redshift cluster.

    Args:
        redshift_client (boto3.client): The Redshift client.
        cluster_name (str): The name of the Redshift cluster.
        snapshot_identifier (str): The identifier for the snapshot.
        retention_period (int): The retention period for the snapshot in days.

    Returns:
        None
    """

    cluster_status = get_cluster_status(redshift_client, cluster_name)
    if cluster_status == 'paused':
        resume_cluster(redshift_client, cluster_name)

    wait_for_cluster_status(redshift_client, cluster_name, 'available')

    logger.info(
        f"Taking final snapshot '{snapshot_identifier}' for cluster '{cluster_name}'...")

    response = redshift_client.create_cluster_snapshot(
        SnapshotIdentifier=snapshot_identifier,
        ClusterIdentifier=cluster_name
    )

    snapshot_status = ''
    while snapshot_status != 'available':
        response = redshift_client.describe_cluster_snapshots(
            SnapshotIdentifier=snapshot_identifier,
            SnapshotType='manual'
        )
        snapshot_status = response['Snapshots'][0]['Status']
        logger.info(
            f"Waiting for snapshot '{snapshot_identifier}' to become available (current status: {snapshot_status})...")
        time.sleep(10)

    logger.info(f"Snapshot '{snapshot_identifier}' is available for use")

    redshift_client.modify_cluster_snapshot(
        SnapshotIdentifier=snapshot_identifier,
        ManualSnapshotRetentionPeriod=retention_period
    )

    logger.info(
        f"Snapshot '{snapshot_identifier}' retention period has been set to {retention_period} days")

    time.sleep(30)


def delete_cluster(redshift_client, cluster_name):
    """
    Terminate the specified Redshift cluster.

    Args:
        redshift_client (botocore.client.Redshift): An instance of boto3 Redshift client.
        cluster_name (str): The name of the Redshift cluster to terminate.

    Returns:
        None
    """

    logger.info(f"Checking cluster state '{cluster_name}'")

    cluster_state = get_cluster_status(redshift_client, cluster_name)

    logger.info(f"Cluster '{cluster_name}' is now in '{cluster_state}' status")

    while cluster_state != 'available':
        time.sleep(10)
        wait_for_cluster_status(redshift_client, cluster_name, 'available')

    logger.info(f"Terminating cluster '{cluster_name}'")

    try:
        response = redshift_client.delete_cluster(
            ClusterIdentifier=cluster_name,
            SkipFinalClusterSnapshot=True
        )

    except Exception as e:
        logger.error(f"There was an error: {e}")
        time.sleep(10)
        delete_cluster(redshift_client, cluster_name)

    cluster_state = get_cluster_status(redshift_client, cluster_name)
    cluster_availability = check_cluster_availability(
        redshift_client, cluster_name)

    while cluster_state != 'deleted' and cluster_availability is True:
        time.sleep(10)
        cluster_availability = check_cluster_availability(
            redshift_client, cluster_name)
        if cluster_availability:
            cluster_state = get_cluster_status(redshift_client, cluster_name)
            logger.info(
                f"Waiting for cluster '{cluster_name}' to be deleted. Current status: '{cluster_state}'")

    logger.info(f"Cluster '{cluster_name}' has been deleted")
    exit(1)


def main():
    """
    The main function for the script. Prompts the user for the Redshift cluster name and AWS credentials profile, checks if the
    cluster is in a paused state, takes a final snapshot with a specified retention period, and terminates the cluster.

    Args:
        None

    Returns:
        None
    """
    cluster_name = input("Enter Redshift cluster name: ")
    profile_name = input(
        "Enter AWS credentials profile name(DEV,TEST,UAT,PROD): ")

    if is_aws_profile_available(profile_name):
        pass
    else:
        exit(1)

    retention_period = 90

    session = boto3.Session(profile_name=profile_name,
                            region_name='ap-south-1')
    redshift_client = session.client('redshift')

    if check_cluster_availability(redshift_client, cluster_name):
        pass
    else:
        exit(1)

    snapshot_identifier = f"{cluster_name}-final-snapshot"
    take_final_snapshot(redshift_client, cluster_name,
                        snapshot_identifier, retention_period)

    delete_cluster(redshift_client, cluster_name)


if __name__ == '__main__':
    main()
