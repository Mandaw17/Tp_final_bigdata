from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

# from pysrc.file_downloader import download_public_drive_zips as download_public_files
# from pysrc.cleaning import clean_network_logs
# from pysrc.feature_engineering.features import build_features
# from pysrc.training.train import train_model
# from pysrc.storage.s3 import upload_to_s3
import pandas as pd
import requests
import os
import re
import sys

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

RAW_DIR = "/tmp/raw"

FOLDER_URL = "https://drive.google.com/drive/folders/1pxNot-Ds72P1bJDY7FCj_kL4BZa0abq7"


# def download_public_files(folder_url: str, output_dir: str):
#     os.makedirs(output_dir, exist_ok=True)

#     response = requests.get(folder_url)
#     response.raise_for_status()

#     html = response.text

    
#     file_ids = set(re.findall(r'"([a-zA-Z0-9_-]{25,})"', html))

#     for file_id in file_ids:
#         download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
#         r = requests.get(download_url, stream=True)

#         content_type = r.headers.get("Content-Type", "")
#         if "zip" not in content_type:
#             continue  

#         filename = f"{file_id}.zip"
#         file_path = os.path.join(output_dir, filename)

#         with open(file_path, "wb") as f:
#             for chunk in r.iter_content(chunk_size=8192):
#                 if chunk:
#                     f.write(chunk)

#         print(f"Downloaded: {filename}")

import boto3
from botocore.exceptions import NoCredentialsError, ClientError

def download_public_files():
    # 1. Configuration du client
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='fipfLf3NXA3qdHALpsaW',
        aws_secret_access_key='BezPqKSXriLa4q13vss1O74VZaT9JPPZdSOg3Rc6',
        region_name='fr-location'
    )

    bucket_name = 'source'
    object_names = ['Network_logs.csv', 'Time-Series_Network_logs.csv']

    for object_name in object_names:
        try:
            s3.download_file(bucket_name, object_name, object_name)
            print(f'Téléchargement réussi de {object_name} vers {object_name}')
        except NoCredentialsError:
            print('Erreur : Identifiants non valides')
        except ClientError as e:
            print(f'Erreur lors du téléchargement de {object_name} : {e}')


def clean_logs(input_path: str, output_path: str):

    df = pd.read_csv(input_path)

    df = df.dropna()

    df.drop(columns=["Port", "Status"], inplace=True)

    df.to_csv(output_path, index=False)

def handle_ips(df):
    import ipaddress

    def is_private(ip):
        try:
            return int(ipaddress.ip_address(ip).is_private)
        except ValueError:
            return 0

    df["src_ip_private"] = df["Source_IP"].apply(is_private)
    df["dst_ip_private"] = df["Destination_IP"].apply(is_private)

    df = df.drop(columns=["Source_IP", "Destination_IP"])

    return df

def build_features(input_path: str, output_path: str):
    df = pd.read_csv(input_path)

    df = handle_ips(df)

    cat_cols = ["Request_Type", "Protocol", "User_Agent", "Scan_Type"]

    df_encoded = pd.get_dummies(
        df[cat_cols],
        prefix=["req", "proto", "ua", "scan"],
        drop_first=True
    )

    df = df.drop(columns=cat_cols)

    df = pd.concat([df, df_encoded], axis=1)
    
    df.head()
    df.to_csv(output_path, index=False)


def train_model(input_path: str, output_path: str):
    labelised_data = pd.read_csv(input_path)
    print(labelised_data.info())
    # set training data
    X = labelised_data.drop(columns=["Intrusion"])
    y = labelised_data["Intrusion"]

    # set test data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

    # initialize model
    model = RandomForestClassifier()

    # train model
    model.fit(X_train, y_train)

    # predict on test set
    predictions = model.predict(X_test)

    print("Predictions on test set:", predictions)

    labelised_data.to_csv(output_path, index=False)
    


with DAG(
    dag_id="public_dataset_ingestion",
    tags=["ingestion", "public-data"],
) as dag:

    download_data = PythonOperator(
        task_id="download_public_files",
        python_callable=download_public_files,
        op_kwargs={
            "folder_url": FOLDER_URL,
            "output_dir": RAW_DIR,
        },
    )

    crete_dirs = BashOperator(
        task_id="create_raw_dir",
        bash_command="mkdir -p /tmp/raw /tmp/unzipped /tmp/clean /tmp/features /tmp/model",
    )

    unzip_files = BashOperator(
        task_id="unzip_files",
        bash_command="unzip -o /tmp/raw/*.zip -d /tmp/unzipped/",
    )

    clean_data = PythonOperator(
        task_id="clean_data",
        python_callable=clean_logs,
        op_kwargs={
            "input_path": f"Network_logs.csv",
            "output_path": "/tmp/clean/clean_logs.csv",
        },
    )

    feature_engineering = PythonOperator(
        task_id="feature_engineering",
        python_callable=build_features,
        op_kwargs={
            "input_path": "/tmp/clean/clean_logs.csv",
            "output_path": "/tmp/features/features.parquet",
        },
    )



    # save_features = PythonOperator(
    #     task_id="save_features",
    #     python_callable=upload_to_s3,
    #     op_kwargs={
    #         "file_path": "/tmp/features/features.parquet",
    #         "bucket": "feature-store",
    #         "key": "navigation/date={{ ds }}/features.parquet",
    #     },
    # )

    train_model = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
        op_kwargs={
            "input_path": "/tmp/features/features.parquet",
            "output_path": "/tmp/model/model.pkl",
        },
    )

    download_data  >> clean_data >> feature_engineering >> train_model 
    # >> save_features




