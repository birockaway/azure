from azure.storage.blob import BlockBlobService
from azure.storage.blob import ContentSettings
from keboola import docker # pro komunikaci s parametrama a input/output mapping

# initialize KBC configuration 
cfg = docker.Config(os.path.abspath(os.path.join(os.getcwd(), os.pardir))+'data/')
# loads application parameters - user defined
parameters = cfg.get_parameters()
account_key = parameters.get('account_key')
account_name = parameters.get('account_name')
destination_container = parameters.get('destination_container')



block_blob_service = BlockBlobService(account_name=, account_key=account_key)
block_blob_service.create_container(destination_container)

#Upload the CSV file to Azure cloud
block_blob_service.create_blob_from_path(
    'mycontainer',
    'test.csv',
    'data/in/tables/test.csv',
    content_settings=ContentSettings(content_type='application/CSV')
            )