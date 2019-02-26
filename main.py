from azure.storage.blob import BlockBlobService
from azure.storage.blob import ContentSettings
from keboola import docker # pro komunikaci s parametrama a input/output mapping
import os

# initialize KBC configuration 
cfg = docker.Config('./data/')
# loads application parameters - user defined
parameters = cfg.get_parameters()
account_key = parameters.get('account_key')
account_name = parameters.get('account_name')
destination_container = parameters.get('destination_container')
in_tables_dir = 'data/in/tables/'


block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)
print(f'Cointainer connected to {account_name} account of BlockBlobService...')

#block_blob_service.create_container(destination_container)

in_tables_list = [i for i in os.listdir(in_tables_dir) if i.endswith('.csv')]
print(f'Uploading tables {in_tables_list} to BlockBlobService...')
#Upload the CSV file to Azure cloud
block_blob_service.create_blob_from_path(
    destination_container,
    'test.csv',
    'data/in/tables/test.csv',
    content_settings=ContentSettings(content_type='application/CSV')
	)
print(f'Tables {in_tables_list} sucessfuly uploaded to BlockBlobService...')