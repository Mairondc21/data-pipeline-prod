import boto3

s3 = boto3.client('s3',
    endpoint_url='http://localhost:3900',
    aws_access_key_id='GK092b24f828f48e9106904881',
    aws_secret_access_key='dcd93f148c32faca0074310ae16845b4f94b9a52f111d2ae07604ac12a8dd7f2',
    region_name='us-east-1'
)

BUCKET = 'raw-data'

try:
    response = s3.list_objects_v2(Bucket=BUCKET)
    
    if 'Contents' in response:
        for obj in response['Contents']:
            print(f"📁 Arquivo: {obj['Key']} | Tamanho: {obj['Size']} bytes")
    else:
        print("bucket vazio")

except Exception as e:
    print("Erro:", e)