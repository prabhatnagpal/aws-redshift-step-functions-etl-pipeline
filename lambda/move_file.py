import json
import boto3
import botocore
import psycopg2
import os

def redshift_data_loader(file_name):
    '''
    This Function Loads the data from validated csv file in redshift_data to RedShift Database
    '''
    client = boto3.client(service_name='redshift', region_name='ap-south-1')

    cluster_creds = client.get_cluster_credentials(
        DbUser=os.environ['dbuser'],
        DbName=os.environ['dbname'],
        ClusterIdentifier=os.environ['cluster_id'],
        AutoCreate=False
    )

    try:
        connection = psycopg2.connect(
            host = os.environ['host'],
            port = '5439',
            dbname = os.environ['dbname'],
            user = cluster_creds['DbUser'],
            password = cluster_creds['DbPassword']
        )
        tablename = os.environ['tablename']
        iam_role = os.environ['iam_role']
        cursor = connection.cursor()
        query = "COPY {} FROM '{}' IAM_ROLE '{}' CSV;".format(tablename,file_name,iam_role)
        cursor.execute(query)
        connection.commit()
        cursor.close()
        connection.close()
    except:
        print("Failed to open database connection")

def lambda_handler(event, context):  

    '''
    This function Moves the source dataset to redshift_data/archive/transform/error folder
    '''
    print(event)
    s3_resource = boto3.resource('s3')
    bucket_name = event['bucket_name']
    result = {}
    if "error-info" in event:
        source_location = "stage"
        status = "FAILURE"
    else:
        source_location = event['taskresult']['Location']
        status = event['taskresult']['Validation']

    file_name = event['file_name']
    key_name = source_location + "/" + file_name

    if status == "FAILURE":
        print("Status is set to failure. Moving to error folder")
        folder = os.environ['error_folder_name']
    elif status == "SUCCESS":
        print("Status is set to archive. Moving to data and archive folder")
        folder = os.environ['archive_folder_name']
        redshift_data_folder = os.environ['redshift_data_folder_name']
    source_file_name_to_copy = bucket_name + "/" + source_location + "/" + file_name
    move_file_name = folder + "/" + file_name
    move_redshift_file_name = redshift_data_folder + "/" + file_name
    print("moving file to " + move_file_name + " and " + move_redshift_file_name)
    s3_resource.Object(bucket_name, move_file_name).copy_from(CopySource=source_file_name_to_copy)
    s3_resource.Object(bucket_name, move_redshift_file_name).copy_from(CopySource=source_file_name_to_copy)
    s3_resource.Object(bucket_name, key_name).delete()
    redshift_data_loader(move_redshift_file_name)
    result['Status'] = status
    result['msg'] = "File moved to " + move_file_name + " and " + move_redshift_file_name
    return(result)