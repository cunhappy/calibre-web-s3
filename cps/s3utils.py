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
import re

from . import logger, config

log = logger.create()

def get_s3_client():
    if not s3_support or not config.config_use_s3:
        # If config is not yet initialized, check environment variables
        if not os.environ.get('S3_USE', '').lower() in ('true', '1', 'yes'):
            return None
        
        # Use env vars if available
        endpoint = os.environ.get('S3_ENDPOINT')
        region = os.environ.get('S3_REGION') or 'us-east-1'
        access_key = os.environ.get('S3_ACCESS_KEY')
        secret_key = os.environ.get('S3_SECRET_KEY')
        bucket = os.environ.get('S3_BUCKET')
        
        if not all([access_key, secret_key, bucket]):
            return None
            
        s3_config = Config(
            region_name=region,
            signature_version='s3v4',
        )
        return boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=s3_config
        )

    log.debug("Initializing S3 client with endpoint: %s, region: %s", 
              config.config_s3_endpoint, config.config_s3_region)
    
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
    
    bucket = config.config_s3_bucket if hasattr(config, 'config_s3_bucket') and config.config_s3_bucket else os.environ.get('S3_BUCKET')
    
    log.info("Uploading %s to S3 bucket %s as %s", 
             file_obj if isinstance(file_obj, str) else "file object", 
             bucket, s3_path)
    try:
        if isinstance(file_obj, str):
            client.upload_file(file_obj, bucket, s3_path)
        else:
            client.upload_fileobj(file_obj, bucket, s3_path)
        log.debug("S3 Upload successful: %s", s3_path)
        return True
    except ClientError as e:
        log.error(f"S3 Upload Error: {e}")
        return False

def download_file(s3_path, local_path):
    client = get_s3_client()
    if not client:
        return False
    
    bucket = config.config_s3_bucket if hasattr(config, 'config_s3_bucket') and config.config_s3_bucket else os.environ.get('S3_BUCKET')
    
    log.info("Downloading %s from S3 bucket %s to %s", 
             s3_path, bucket, local_path)
    try:
        client.download_file(bucket, s3_path, local_path)
        log.debug("S3 Download successful: %s", local_path)
        return True
    except ClientError as e:
        log.error(f"S3 Download Error: {e}")
        return False

def download_metadata_db():
    if not config.config_use_s3:
        # Try to check environment variables if config is not yet initialized
        if not os.environ.get('S3_USE', '').lower() in ('true', '1', 'yes'):
            return False
    
    calibre_dir = (config.config_calibre_dir if hasattr(config, 'config_calibre_dir') else None) or os.environ.get('CALIBRE_DBPATH')
    if not calibre_dir:
        return False
    
    # Strip metadata.db from the end if present
    calibre_dir = re.sub(r'metadata\.db$', '', calibre_dir).rstrip(os.sep)

    if not os.path.exists(calibre_dir):
        try:
            os.makedirs(calibre_dir)
            log.info("Created Calibre directory: %s", calibre_dir)
        except OSError as e:
            log.error("Failed to create Calibre directory %s: %s", calibre_dir, e)
            return False

    metadata_db_path = os.path.join(calibre_dir, "metadata.db")
    print(f"Attempting to download metadata.db from S3 to {metadata_db_path}", flush=True)
    log.info("Attempting to download metadata.db from S3 to %s", metadata_db_path)
    return download_file("metadata.db", metadata_db_path)

def download_app_db(settings_path):
    # This is called before the config is fully loaded from the DB
    if not os.environ.get('S3_USE', '').lower() in ('true', '1', 'yes'):
        return False
    
    print(f"Attempting to download app.db from S3 to {settings_path}", flush=True)
    log.info("Attempting to download app.db from S3 to %s", settings_path)
    # We need to ensure the directory exists
    settings_dir = os.path.dirname(settings_path)
    if settings_dir and not os.path.exists(settings_dir):
        try:
            os.makedirs(settings_dir)
        except OSError as e:
            log.error("Failed to create settings directory %s: %s", settings_dir, e)
            return False
            
    return download_file("app.db", settings_path)

def sync_app_db(settings_path):
    if not config.config_use_s3:
        return False
    if os.path.exists(settings_path):
        upload_file(settings_path, "app.db")
        return True
    return False

def generate_presigned_url(s3_path, expiration=3600, filename=None):
    client = get_s3_client()
    if not client:
        return None
    
    bucket = config.config_s3_bucket if hasattr(config, 'config_s3_bucket') and config.config_s3_bucket else os.environ.get('S3_BUCKET')
    params = {'Bucket': bucket, 'Key': s3_path}
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
    
    bucket = config.config_s3_bucket if hasattr(config, 'config_s3_bucket') and config.config_s3_bucket else os.environ.get('S3_BUCKET')
    try:
        obj = client.get_object(Bucket=bucket, Key=s3_path)
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
    
    bucket = config.config_s3_bucket if hasattr(config, 'config_s3_bucket') and config.config_s3_bucket else os.environ.get('S3_BUCKET')
    try:
        paginator = client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
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
    
    bucket = config.config_s3_bucket if hasattr(config, 'config_s3_bucket') and config.config_s3_bucket else os.environ.get('S3_BUCKET')
    try:
        client.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': old_key},
            Key=new_key
        )
        client.delete_object(Bucket=bucket, Key=old_key)
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
    
    bucket = config.config_s3_bucket if hasattr(config, 'config_s3_bucket') and config.config_s3_bucket else os.environ.get('S3_BUCKET')
    try:
        client.delete_object(Bucket=bucket, Key=key)
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
    calibre_dir = (config.config_calibre_dir if hasattr(config, 'config_calibre_dir') else None) or os.environ.get('CALIBRE_DBPATH')
    if not calibre_dir:
        return False
    calibre_dir = re.sub(r'metadata\.db$', '', calibre_dir).rstrip(os.sep)
    metadata_db_path = os.path.join(calibre_dir, "metadata.db")
    if os.path.exists(metadata_db_path):
        upload_file(metadata_db_path, "metadata.db")
        return True
    return False
