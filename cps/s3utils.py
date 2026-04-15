# -*- coding: utf-8 -*-

try:
    import boto3
    from botocore.exceptions import ClientError
    from botocore.config import Config
    s3_support = True
except ImportError:
    s3_support = False

from flask import stream_with_context, Response
import os

from . import logger, config

log = logger.create()

def get_s3_client():
    if not s3_support or not config.config_use_s3:
        return None
    
    s3_config = Config(
        region_name=config.config_s3_region or 'us-east-1',
        signature_version='s3v4',
    )
    
    return boto3.client(
        's3',
        endpoint_url=config.config_s3_endpoint,
        aws_access_key_id=config.config_s3_access_key,
        aws_secret_access_key=config.config_s3_secret_key_e,
        config=s3_config
    )

def upload_file(file_obj, s3_path):
    client = get_s3_client()
    if not client:
        return False
    
    try:
        if isinstance(file_obj, str):
            client.upload_file(file_obj, config.config_s3_bucket, s3_path)
        else:
            client.upload_fileobj(file_obj, config.config_s3_bucket, s3_path)
        return True
    except ClientError as e:
        log.error(f"S3 Upload Error: {e}")
        return False

def download_file(s3_path, local_path):
    client = get_s3_client()
    if not client:
        return False
    
    try:
        client.download_file(config.config_s3_bucket, s3_path, local_path)
        return True
    except ClientError as e:
        log.error(f"S3 Download Error: {e}")
        return False

def generate_presigned_url(s3_path, expiration=3600, filename=None):
    client = get_s3_client()
    if not client:
        return None
    
    params = {'Bucket': config.config_s3_bucket, 'Key': s3_path}
    if filename:
        params['ResponseContentDisposition'] = f'attachment; filename="{filename}"'
    
    try:
        url = client.generate_presigned_url(
            'get_object',
            Params=params,
            ExpiresIn=expiration
        )
        return url
    except ClientError as e:
        log.error(f"S3 Presigned URL Error: {e}")
        return None

def get_file_stream(s3_path):
    client = get_s3_client()
    if not client:
        return None
    
    try:
        obj = client.get_object(Bucket=config.config_s3_bucket, Key=s3_path)
        return obj['Body']
    except ClientError as e:
        log.error(f"S3 Stream Error: {e}")
        return None

def stream_s3_file(s3_path, headers):
    stream = get_file_stream(s3_path)
    if not stream:
        return None
    
    def generate():
        for chunk in stream.iter_chunks(chunk_size=4096):
            yield chunk
            
    return Response(stream_with_context(generate()), headers=headers)

def list_objects(prefix):
    client = get_s3_client()
    if not client:
        return []
    
    try:
        paginator = client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=config.config_s3_bucket, Prefix=prefix)
        
        objects = []
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    objects.append(obj['Key'])
        return objects
    except ClientError as e:
        log.error(f"S3 List Error: {e}")
        return []

def move_object(old_key, new_key):
    client = get_s3_client()
    if not client:
        return False
    
    try:
        client.copy_object(
            Bucket=config.config_s3_bucket,
            CopySource={'Bucket': config.config_s3_bucket, 'Key': old_key},
            Key=new_key
        )
        client.delete_object(Bucket=config.config_s3_bucket, Key=old_key)
        return True
    except ClientError as e:
        log.error(f"S3 Move Error: {e}")
        return False

def move_folder(old_prefix, new_prefix):
    if not old_prefix.endswith('/'):
        old_prefix += '/'
    if not new_prefix.endswith('/'):
        new_prefix += '/'
        
    objects = list_objects(old_prefix)
    for obj_key in objects:
        new_key = obj_key.replace(old_prefix, new_prefix, 1)
        move_object(obj_key, new_key)
    return True

def delete_object(key):
    client = get_s3_client()
    if not client:
        return False
    
    try:
        client.delete_object(Bucket=config.config_s3_bucket, Key=key)
        return True
    except ClientError as e:
        log.error(f"S3 Delete Error: {e}")
        return False

def delete_folder(prefix):
    if not prefix.endswith('/'):
        prefix += '/'
    
    objects = list_objects(prefix)
    for obj_key in objects:
        delete_object(obj_key)
    return True

def sync_metadata_db():
    metadata_db_path = os.path.join(config.config_calibre_dir, "metadata.db")
    if os.path.exists(metadata_db_path):
        upload_file(metadata_db_path, "metadata.db")
        return True
    return False
