import mysql.connector
import os
import requests
import json
import boto3
import time
import re
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
def connect_preprod():
    db = mysql.connector.connect(
        host=os.environ.get('preprod-host'),
        user=os.environ.get('preprod-admin-user'),
        passwd=os.environ.get('preprod-admin-pass'),
        database=os.environ.get('preprod-database'),
    )

    cursor = db.cursor()
    return cursor, db

constring = f'mysql://{os.environ.get("preprod-admin-user")}:{os.environ.get("preprod-admin-pass")}@{os.environ.get("preprod-host")}:3306/{os.environ.get("preprod-database")}'

def pipeline_messenger(title, text, hexcolour_value):
    messenger_colours = {
        'pass': '#00c400',
        'fail': '#c40000',
        'notification': '#0000c4'
    }
    url = "https://tdworldwide.webhook.office.com/webhookb2/d5d1f4d1-2858-48a6-8156-5abf78a31f9b@7fe14ab6-8f5d-4139-84bf-cd8aed0ee6b9/IncomingWebhook/76b5bd9cd81946338da47e0349ba909d/c5995f3f-7ce7-4f13-8dba-0b4a7fc2c546"
    payload = json.dumps({
        "@type": "MessageCard",
        "themeColor": messenger_colours[hexcolour_value],
        "title": title,
        "text": text,
        "markdown": True
    })
    headers = {
        'Content-Type': 'application/json'
    }
    requests.request("POST", url, headers=headers, data=payload)


def create_s3_connection() -> boto3.client:
    s3client = boto3.client('s3',
                            aws_access_key_id=os.environ.get('aws-access-key-id-data-services'),
                            aws_secret_access_key=os.environ.get('aws-secret-key-data-services'),
                            region_name=os.environ.get('aws-region')
                            )
    buckets = s3client.list_buckets()
    return s3client


def download_file(client: boto3, filename: str, target_bucket: str, local_folder: str='') -> None:
    """
    download a filename from a target_bucket into a local folder+filename
    :param local_folder:
    :param client:
    :param filename:
    :param target_bucket:
    :return:
    """
    logger.info('downloading {} from bucket {}, the file is targeted locally as {}/{}'.format(filename,
                                                                                              target_bucket,
                                                                                              local_folder,
                                                                                              filename))
    t0 = time.time()
    destination_folder = local_folder + '/' + filename
    client.download_file(Filename=destination_folder, Bucket=target_bucket, Key=filename)
    t1 = time.time()
    logger.info(f'download took {round(t1 - t0)} seconds')


def upload_file(client: boto3.client, filename: str, target_bucket: str) -> None:
    """
    send a file to s3 bucket
    :param target_bucket:
    :param client:
    :param filename:
    :return:
    """

    # remove folders to provide just filename when uploading
    target_file_name = re.search(r".*/([^/]+)$", filename)
    if target_file_name:
        target_file_name = target_file_name.group(1)
    else:
        target_file_name = filename

    logger.info('uploading {} to {} as {}'.format(filename, target_bucket, target_file_name))

    t0 = time.time()
    client.upload_file(Filename=filename, Bucket=target_bucket, Key=target_file_name)
    t1 = time.time()
    logger.info(f'upload took {round(t1 - t0)} seconds, check {target_bucket} for {target_file_name}')

s3_conn = create_s3_connection()

filename = '2024-07-01-StockUniteLegale_utf8.zip'

if __name__ == '__main__':
    upload_file(s3_conn, filename, 'iqblade-data-services-sirene-incoming-files')
