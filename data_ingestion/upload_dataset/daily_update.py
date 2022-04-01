# Daily Update will started from 2022-01-01 - Present
from asyncio import constants
import os
from time import sleep
import pandas as pd
from google.cloud import storage
import datetime

BUCKET_NAME = os.environ.get("GCP_GCS_BUCKET")
URL_DATA_SOUCE = pd.read_csv('./data_source_url.csv')


def get_dataframe_from_url(url: str):
    dataframe = pd.read_csv(url)
    return dataframe


def create_url_for_download(product_id: str, from_date: str, to_date: str):
    # https://data.moc.go.th/OpenData/GISProductPrice?product_id=P11001&from_date=2022-03-01&to_date=2022-03-30&task=export_csv
    base_url = 'https://data.moc.go.th/OpenData/GISProductPrice?'
    query = f'product_id={product_id}&from_date={from_date}&to_date={to_date}&task=export_csv'
    return f'{base_url}{query}'


def upload_dataframe_to_gcs(dataframe, bucket_name, destination_blob_name):
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client.from_service_account_json(
        json_credentials_path="./services_account.json")

    bucket = client.bucket(bucket_name)

    bucket.blob(destination_blob_name).upload_from_string(
        dataframe.to_csv(), 'text/csv')

    print(f'Upload file to {destination_blob_name} completed')


def download_and_upload_to_gcs(product_id: str, from_date: str, to_date: str):
    download_url = create_url_for_download(product_id, from_date, to_date)
    dataframe = get_dataframe_from_url(download_url)
    print(dataframe)
    # rename column name to english
    dataframe.rename(columns={
        "รหัสสินค้า": 'product_id',
        'ชื่อสินค้า': "product_name",
        'หมวดหมู่สินค้า': "product_category",
        'กลุ่มสินค้า': "product_group",
        'วันที่สำรวจ': 'recored_date',
        'ราคาต่ำสุด': 'min_price',
        'ราคาสูงสุด': 'high_price',
    }, inplace=True)
    file_name = f"{product_id}_{from_date}_{to_date}"
    destination_blob_name = f"{product_id}/{file_name}.csv"
    upload_dataframe_to_gcs(dataframe, BUCKET_NAME,
                            destination_blob_name)


def daily_update(from_date: str, to_date: str):
    # read list production id for initial dataset to project
    product_df = pd.read_csv("./data_source_url.csv")
    product_id_list = product_df['product_id']
    for product_id in product_id_list:
        download_and_upload_to_gcs(product_id, from_date, to_date)
        sleep(3)


today = datetime.datetime.now().strftime("%Y-%m_%d")
daily_update(today, today)
