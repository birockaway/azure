from azure.storage.blob import BlockBlobService
from azure.storage.blob import ContentSettings
from keboola import docker # pro komunikaci s parametrama a input/output mapping
import os
from datetime import datetime
import pandas as pd

in_tables_dir = '/data/in/tables/'
in_config_dir = '/data/'
date_col_default = 'date'
suffix_delimiter = '-'

# get KBC parameters
cfg = docker.Config(in_config_dir)
# loads application parameters - user defined
parameters = cfg.get_parameters()
account_key = parameters.get('account_key')
account_name = parameters.get('account_name')
destination_container = parameters.get('destination_container')
table_name_suffix_type = parameters.get('table_name_suffix_type')
date_col = parameters.get('date_col')

if not date_col:
	date_col = date_col_default

def get_suffix(table_name, table_name_suffix_type):
	"""
	Get suffix for the destination table name.
	"""
	if table_name_suffix_type == 'snapshot':		
		table_name_suffix = suffix_delimiter + datetime.now().strftime("%Y%m%d%H%M%S")
	elif table_name_suffix_type == 'max_date':
		try:
			table_dates_df = pd.read_csv(in_tables_dir+table_name, usecols=[date_col])
			table_name_suffix = suffix_delimiter + str(table_dates_df[date_col].max())
			print(f"Suffix based on maximum of '{date_col}' column in {table_name} table.")
		except:
			table_name_suffix = ''
			print(f"Column '{date_col}' was not found in {table_name} table.")
	else:
		table_name_suffix = ''		
	print(f"The {table_name} table's suffix was set as '{table_name_suffix}'")

	return table_name_suffix

block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)
print(f'Docker cointainer will try to connect to {account_name} account of BlockBlobService...')

# TODO:
# Create the container if it does not exist.
#block_blob_service.create_container(destination_container)

in_tables_list = [os.path.splitext(i)[0] for i in os.listdir(in_tables_dir) if i.endswith('.csv')]
print(f"Tables to be uploaded: {in_tables_list}")
print(f'Uploading tables {in_tables_list} to {destination_container} storage container of BlockBlobService...')

#Upload the CSV file to Azure cloud
def write_table(block_blob_service, destination_container, table_name, table_name_suffix):
	"""
	write the table to blob storage.
	"""	
	block_blob_service.create_blob_from_path(
	    destination_container,
	    table_name + table_name_suffix + '.csv',
	    in_tables_dir + table_name + '.csv',
	    content_settings=ContentSettings(content_type='application/CSV')
		)

for table_name in in_tables_list:
	try:
		write_table(block_blob_service, destination_container, table_name, get_suffix(table_name, table_name_suffix_type))
		print(f'Table {table_name} sucessfuly uploaded to {destination_container} storage container of BlockBlobService...')
	except Exception as e:
		print(f'Something went wrong during {table_name} table upload...')
		print(f"Exception: {str(e)}")

print('Job finished.')