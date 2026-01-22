import boto3
from botocore.exceptions import NoCredentialsError, ClientError

def download_with_boto3():
    # 1. Configuration du client
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000', # Adresse de votre MinIO
        aws_access_key_id='minoadmin',
        aws_secret_access_key='minoadmin',
        region_name='fr-location' # MinIO ignore souvent la région, mais elle est requise par boto3
    )

    bucket_name = 'source'
    object_names = ['Network_logs.csv', 'Times-Series_Network_logs.csv']

    for object_name in object_names:
        try:
            s3.download_file(bucket_name, object_name, object_name)
            print(f'Téléchargement réussi de {object_name} vers {object_name}')
        except NoCredentialsError:
            print('Erreur : Identifiants non valides')
        except ClientError as e:
            print(f'Erreur lors du téléchargement de {object_name} : {e}')