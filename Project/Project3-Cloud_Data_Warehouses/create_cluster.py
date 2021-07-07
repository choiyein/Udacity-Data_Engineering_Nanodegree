import pandas as pd
import boto3
from botocore.exceptions import ClientError
import json
import configparser
import botocore.exceptions

######################## Getter functions ################################# 
def get_AWS_credential():
    """
    Description:
        Returns a list contained AWS access key id and secret
    Arguments: 
        None
    Return: 
        List: 
            Index 0: access key id 
            Index 1: secret
    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    return [config.get('AWS','KEY'), config.get('AWS','SECRET')]

def get_iam_role_name():
    """
    Description:
        Returns an IAM ROLE name.
    Arguments: 
        None
    Return: 
        String: IAM ROLE NAME 
    """
    # Open configuration file 
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    return config.get("CLUSTER", "DB_IAM_ROLE_NAME")

def get_DB_config():
    """
    Description:
        Returns a dictionary contained cluster type, node number, node type, 
        unique identifier, name, password, user, port, region for a RedShift cluster
    Arguments: 
        None
    Return:
        Dict: 
            KEYS: DB_CLUSTER_TYPE 
                  DB_NUM_NODES 
                  DB_NODE_TYPE
                  DB_CLUSTER_IDENTIFIER
                  DB_NAME
                  DB_USER
                  DB_PORT
                  DB_REGION
    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg')) 

    db_dict = {}
    settings = ["DB_CLUSTER_TYPE",
                "DB_NUM_NODES",
                "DB_NODE_TYPE", 
                "DB_CLUSTER_IDENTIFIER", 
                "DB_NAME", 
                "DB_USER", 
                "DB_PASSWORD", 
                "DB_PORT",
                "DB_REGION"]
    for setting in settings:
       db_dict[setting] = config.get("CLUSTER",setting) 

    return db_dict

def get_ARN (iam_client):
    """
    Description: 
        Returns ARN that uniquely identify AWS resources
    Arguments:
       a low-level client representing AWS IAM 
    Return:
        String: ARN
    """
    roleArn = iam_client.get_role(RoleName=get_iam_role_name())['Role']['Arn']
    return roleArn
###############################################################################
  
def create_client_resource (service, type):
    """
    Description: 
        Returns AWS client or AWS resource
    Arguments:
       Service: str, service that you want to use
       Type: str, client or resource
    Return:
        Client or Resource: 
                client to provide low level AWS service access or 
                resource to provide higher level service access
    """
    KEY, SECRET = get_AWS_credential()
    db_config = get_DB_config()
    if type.lower() == 'resource':
        api = boto3.resource(service,
                        region_name=db_config['DB_REGION'],
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )
    elif type.lower() == 'client':
        api = boto3.client(service,
                        region_name=db_config['DB_REGION'],
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )
    return api 

def create_iam_role (iam_client):
    """
    Description:
        Create a new IAM role
    Arguments:
        Iam_client: client representing IAM service
    Return:
        Dict
    """
    role = iam_client.create_role( 
            Path='/',
            RoleName= get_iam_role_name(),
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                 'Effect': 'Allow',
                 'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
                )    
    return role
    
def attach_policy (iam_client):
    """
    Description:
        Attach the specified managed policy to the specified IAM role. 
    Arguments:
        Iam_client: client representing IAM service
    Return:
        None
    """
    iam_client.attach_role_policy(RoleName=get_iam_role_name(),
                                  PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                                 )['ResponseMetadata']['HTTPStatusCode']


def create_redshift_cluster (iam_client, redshift_client):
    """
    Description:
        Creates a new Redshift cluster.
    Arguments:
        Iam_client: client representing IAM service
        Redshift_client: client representing Redshift service
    Return:
        Dict
    """
    db_config = get_DB_config()
    response = redshift_client.create_cluster(        
                                            #HW
                                            ClusterType= db_config["DB_CLUSTER_TYPE"],
                                            NodeType=db_config["DB_NODE_TYPE"],
                                            NumberOfNodes=int(db_config["DB_NUM_NODES"]),

                                            #Identifiers & Credentials
                                            DBName=db_config["DB_NAME"],
                                            ClusterIdentifier=db_config["DB_CLUSTER_IDENTIFIER"],
                                            MasterUsername=db_config["DB_USER"],
                                            MasterUserPassword=db_config["DB_PASSWORD"],
        
                                            #Roles (for s3 access)
                                            IamRoles=[get_ARN(iam_client)]  
                                            )
    return response

def wait_cluster_available(redshift_client):
    """
    Description:
        Wait for Redshift cluster to be created
    Arguments:
       Redshift_client: client representing Redshift service 
    Return:
        None
    """
    db_config = get_DB_config()
    waiter = redshift_client.get_waiter('cluster_available')
    waiter.wait(ClusterIdentifier=db_config["DB_CLUSTER_IDENTIFIER"])

def describe_cluster(redshift_client):
    """
    Description:
        Return properties of provisioned clusters.
    Arguments:
       Redshift_client: client representing Redshift service 
    Return:
        Dict
    """
    db_config = get_DB_config()
    myCluster = redshift_client.describe_clusters(ClusterIdentifier=db_config["DB_CLUSTER_IDENTIFIER"])['Clusters'][0]
    return myCluster

def write_endpoint_arn(redshift_client):
    """
    Description:
        Write an endpoint of provisioned Redshift cluster, a URL of the entry point for Redshift, 
        and an identifier of the Redshift cluster on the congifuration file named **dwh.cfg**
    Arguments:
       Redshift_client: client representing Redshift service 
    Return:
        None
    """

    # Get configuration for redshift cluster
    rdshift = describe_cluster(redshift_client)

    with open('dwh.cfg', 'r') as file:
        contents = file.read().split('\n')
        file.close()

    new = ""
    for line in contents:
        line.strip()
        if line[:4] == 'HOST':
            line = 'HOST={}'.format(rdshift['Endpoint']['Address'])
        elif 'ARN=' in line:
            line = 'ARN={}'.format(rdshift['IamRoles'][0]['IamRoleArn'])
        line += '\n'
        new += line

    with open('dwh.cfg', 'w') as file:
        file.write(new)
        file.close()

def open_tcp(ec2_client, redshift_client):
    """
    Description:
        Add an ingress rules to defult security group to get an access to the Redshift cluster.
    Arguments:
        EC2_client: client representing EC2 sevice
        Redshift_client: client representing Redshift service 
    Return:
        None
    """
    try:
        rdshift = describe_cluster(redshift_client)
        db_config = get_DB_config()
        vpc = ec2_client.Vpc(id=rdshift['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        defaultSg.authorize_ingress(
                GroupName=defaultSg.group_name,
                CidrIp='0.0.0.0/0',
                IpProtocol='TCP',
                FromPort=int(db_config['DB_PORT']),
                ToPort=int(db_config['DB_PORT'])
        )
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'InvalidPermission.Duplicate':
            pass

def main():

    print("Create clients for IAM, EC2, and Redshift")
    ec2 = create_client_resource('ec2', 'resource')   
    iam = create_client_resource('iam', 'client')
    redshift = create_client_resource('redshift', 'client')
    
    print("Create IAM role")
    create_iam_role(iam) 
 
    print("Attach policy to the IAM role")
    attach_policy(iam)

    print("Create redshift cluster")
    create_redshift_cluster(iam, redshift)

    print("Wait redshift cluster available")
    wait_cluster_available(redshift)

    print("Write dwh end point and ARN")
    write_endpoint_arn(redshift)

    print("Open an incoming TCP port to access the cluster ednpoint")
    open_tcp(ec2, redshift)

if __name__ == "__main__":
    main()