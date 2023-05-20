import boto3
import os
import botocore
import os.path
import pandas


def create_key_pair():
    ec2_client = boto3.client("ec2", region_name="us-east-1")
    key_pair = ec2_client.create_key_pair(KeyName="ec2-key-pair")
    private_key = key_pair["KeyMaterial"]
    with os.fdopen(os.open("aws_ec2_key.pem", os.O_WRONLY | os.O_CREAT, 0o400), "w+") as handle:
        handle.write(private_key)


def create_instance():
    ec2_client = boto3.client("ec2", region_name="us-east-1")
    instances = ec2_client.run_instances(
        ImageId="ami-04505e74c0741db8d",
        MinCount=1,
        MaxCount=1,
        InstanceType="t2.micro",
        KeyName="ec2-key-pair"
    )
    print(instances["Instances"][0]["InstanceId"])


def get_public_ip(instance_id):
    ec2_client = boto3.client("ec2", region_name="us-east-1")
    ip = []
    reservations = ec2_client.describe_instances(InstanceIds=[instance_id]).get("Reservations")
    for reservation in reservations:
        for instance in reservation['Instances']:
            ip.append(instance.get("PublicIpAddress"))
    return ip


def get_running_instances():
    ec2_client = boto3.client("ec2", region_name="us-east-1")
    reservations = ec2_client.describe_instances(Filters=[
        {
            "Name": "instance-state-name",
            "Values": ["running"],
        },
        {
            "Name": "instance-type",
            "Values": ["t2.micro"]
        }
    ]).get("Reservations")
    for reservation in reservations:
        for instance in reservation["Instances"]:
            instance_id = instance["InstanceId"]
            instance_type = instance["InstanceType"]
            public_ip = instance["PublicIpAddress"]
            private_ip = instance["PrivateIpAddress"]
            print(f"{instance_id}, {instance_type}, {public_ip}, {private_ip}")


def ssh():
    tmp = get_public_ip("i-065aa9c120886c977")
    print(f"command ssh: ssh -i aws_ec2_key.pem ec2-user@{tmp[0]}")


def stop_instance(instance_id):
    ec2_client = boto3.client("ec2", region_name="us-east-1")
    response = ec2_client.stop_instances(InstanceIds=[instance_id])
    return response


def start_instance(instance_id):
    ec2_client = boto3.client("ec2", region_name="us-east-1")
    response = ec2_client.start_instances(InstanceIds=[instance_id])
    return response


def terminate_instance(instance_id):
    ec2_client = boto3.client("ec2", region_name="us-east-1")
    response = ec2_client.terminate_instances(InstanceIds=[instance_id])
    return response


def get_instance_info(instance_id):
    ec2_client = boto3.client("ec2", region_name="us-east-1")
    response = ec2_client.describe_instance_status(InstanceIds=[instance_id])
    return response


def bucket_exists(bucket_name):
    s3_client = boto3.resource('s3')
    if s3_client.Bucket(bucket_name) not in s3_client.buckets.all():
        return False
    return True

def bucket_element_exists(bucket_name, s3_obj_name):
    s3_client = boto3.client('s3')
    try:
        s3_client.get_object(Bucket = bucket_name, Key = s3_obj_name)
    except:
        return False
    return True


def create_bucket(bucket_name, region):
    try:
        s3_client = boto3.client('s3', region_name=region)
        location = {'LocationConstraint': region}
    except ValueError:
        print("Error!!!")
        return
    try:
        response = s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
        print(response)
    except botocore.exceptions.ClientError:
        print("Error, such backet is already exists")
        return
    except botocore.exceptions.ParamValidationError:
        print("Error, invalid name. Bucket name must contain only letters, numbers and '-'")
        return


def buckets_list():
    s3 = boto3.client('s3')
    response = s3.list_buckets()
    print('Existing buckets:')
    for bucket in response['Buckets']:
        print(f' {bucket["Name"]}')


def upload(file_name, bucket_name, s3_obj_name):
    s3_client = boto3.client('s3')
    if not bucket_exists(bucket_name):
        print(F"Error. No such bucket {bucket_name}")
        return
    if not os.path.exists(file_name):
        print(f"{file_name}:: such file does not exists")
        return
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_obj_name)
    except:
        response = s3_client.upload_file(Filename=file_name, Bucket=bucket_name, Key=s3_obj_name)
        print(response)
        return
    print(f"{s3_obj_name} is already exists on {bucket_name}")


def read_csv_from_bucket(bucket_name, s3_obj_name):
    s3_client = boto3.client('s3')
    if not bucket_exists(bucket_name):
        print(F"Error. No such bucket {bucket_name}")
        return
    if not bucket_element_exists(bucket_name, s3_obj_name):
        print(F"Error. No such file {s3_obj_name}")
        return
    obj = s3_client.get_object(
        Bucket=bucket_name,
        Key=s3_obj_name
    )
    data = pandas.read_csv(obj['Body'])
    print('Printing the data frame...')
    print(data.head())


def destroy_bucket(bucket_name):
    if not bucket_exists(bucket_name):
        print(F"Error. No such bucket {bucket_name}")
        return
    s3_client = boto3.client('s3')
    response = s3_client.delete_bucket(Bucket=bucket_name)
    print(response)

read_csv_from_bucket('oliferchuk-lab2', 'data.csv')
