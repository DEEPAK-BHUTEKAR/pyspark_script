from pyspark.sql import *
from pyspark.sql.functions import *
import boto3


client=boto3.client('rds',region_name='ap-south-1')
#-----------------boto3 and rds interface------------------------------


#creating db-instance rds using boto3
response = client.create_db_instance(
    DBName='mysqldb',
    DBInstanceIdentifier='boto3mysql',
    AllocatedStorage=20,
    DBInstanceClass='db.t3.micro',
    Engine='mysql',
    MasterUsername='myuser',
    MasterUserPassword='mypassword',

    VpcSecurityGroupIds=[
        'sg-01f7a7c12c43c9e25'
    ]
)

#deleting db-instance
response = client.delete_db_instance(
    DBInstanceIdentifier='boto3mysql',
    SkipFinalSnapshot=True,
    DeleteAutomatedBackups=True
)





