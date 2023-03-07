from pyspark.sql import *
from pyspark.sql.functions import *


import boto3
#-----------------boto3 and rds ec2------------------------------

#--create ec2 instance
'''
Ubuntu ImageId:"ami-0f8ca728008ff5af4"
RedHat ImageId: "ami-0e07dcaca348a0e68"
SUSE Linux    : "ami-05972b154774b3b6c"
'''

'''

ec2 = boto3.resource('ec2',region_name='ap-south-1')

instances = ec2.create_instances(
        ImageId="ami-0f8ca728008ff5af4",
        MinCount=1,
        MaxCount=1,
        InstanceType="t2.micro",
        KeyName="sparkkp"
    )


'''

ec2 = boto3.client('ec2',region_name='ap-south-1')

response = ec2.terminate_instances(
    InstanceIds=[
        'i-0243caffab7816cd0'
    ]
)

