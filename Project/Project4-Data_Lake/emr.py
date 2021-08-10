import configparser
import boto3

def create_interface (type, service):
    """
    Description: 
        Returns AWS client or AWS resource
    Arguments:
       Service:  AWS service
       Type: client or resource
    Return:
        Client or Resource: 
                client to provide low level AWS service access or 
                resource to provide higher level service acces
    """
    config = configparser.ConfigParser()
    config.read_file(open('dl.cfg'))

    if type.lower() == 'resource':
        api = boto3.resource(service,
                        region_name= config.get('EMR', 'EMR_REGION'),
                        aws_access_key_id=config.get('AWS', 'AWS_ACCESS_KEY_ID'),
                        aws_secret_access_key=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
                        )
    elif type.lower() == 'client':
        api = boto3.client(service,
                        region_name= config.get('EMR', 'EMR_REGION'),
                        aws_access_key_id=config.get('AWS', 'AWS_ACCESS_KEY_ID'),
                        aws_secret_access_key=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
                        )
    return api 

def setup_bucket(s3_resource):
    """
    Description: 
        Creates an Amazon S3 bucket and uploads the specified script file to it.
    Arguments:
        s3_resource: The Boto3 Amazon S3 resource object.
    Return:
        The newly created bucket.
    """
    config = configparser.ConfigParser()
    config.read_file(open('dl.cfg'))

    bucket = s3_resource.create_bucket(
        ACL='public-read-write',
        Bucket=config.get('S3', 'BUCKET_NAME'),
        CreateBucketConfiguration={
            'LocationConstraint': config.get('EMR', 'EMR_REGION') 
        }
    )
    bucket.wait_until_exists()
    s3_resource.meta.client.upload_file('./etl.py', config.get('S3', 'BUCKET_NAME'), 'scripts/etl.py')
    return bucket

def run_job_flow(emr_client):
    """
    Description: 
        Runs a job flow with the specified steps. A job flow creates a cluster of
        instances and adds steps to be run on the cluster. Steps added to the cluster
        are run as soon as the cluster is ready.
    Arguments:
        job_flow_role: The IAM role assumed by the cluster.
        service_role: The IAM role assumed by the service.
        emr_client: The Boto3 EMR client object.
    Return:
        The ID of the newly created cluster.
    """

    config = configparser.ConfigParser()
    config.read_file(open('dl.cfg'))

    response = emr_client.run_job_flow(
            Name='udacityDataLakeProject',
            LogUri='s3://{}/logs'.format(config.get('S3', 'BUCKET_NAME')),
            ReleaseLabel='emr-5.30.1',
            Instances={
                'MasterInstanceType': 'm5.xlarge',
                'SlaveInstanceType': 'm5.xlarge',
                'InstanceCount': int(config.get('EMR', 'NUM_NODE')),
                'KeepJobFlowAliveWhenNoSteps': False
            },
            Steps=[{
                'Name': 'Run Spark',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit', 's3://{}/scripts/etl.py'.format(config.get('S3', 'BUCKET_NAME'))]
                }
            }],
            Applications=[{
                'Name': 'Spark'
            }],
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            VisibleToAllUsers=True
        )
    cluster_id = response['JobFlowId']
    return cluster_id


def main():

    print("Create clients for EMR and S3")
    emr = create_interface('client', 'emr')
    s3 = create_interface('resource', 's3')

    print("Create S3 bucket")
    setup_bucket(s3)
    
    print("Run PySpark")
    cluster_id=run_job_flow(emr)

    print("cluster_id: " + cluster_id)

if __name__ == "__main__":
    main()


