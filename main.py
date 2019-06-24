from azure.storage.blob import BlockBlobService
from azure.storage.blob.baseblobservice import BaseBlobService
from azure.storage.blob import ContentSettings
from keboola import docker # pro komunikaci s parametrama a input/output mapping
import os
from datetime import datetime
import pandas as pd
import json
import os, shutil

in_tables_dir = '/data/in/tables/'
out_tables_dir = '/data/out/tables/'
out_data_dir = '/data/out/'
in_config_dir = '/data/'
date_col_default = 'date'
suffix_delimiter = '-'
csv_suffix = '.csv'
config_suffix = '.config'

# get KBC parameters
cfg = docker.Config(in_config_dir)
# loads application parameters - user defined
parameters = cfg.get_parameters()
account_key = parameters.get('account_key')
account_name = parameters.get('account_name')
data_container = parameters.get('data_container')
config_container = parameters.get('config_container')
date_col = parameters.get('date_col')

if not date_col:
	date_col = date_col_default

block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)
base_blob_service = BaseBlobService(account_name=account_name, account_key=account_key)
print(f'Docker cointainer will try to connect to {account_name} account of BlockBlobService...')

def download_config(base_blob_service, config_container, table_name):
	config_name = table_name + config_suffix
	config_path = out_data_dir + config_name
	try:
		base_blob_service.get_blob_to_path(config_container, config_name, config_path)
	except:
		brand_new_config = {}
		brand_new_config['latest'] = '19700101'
		with open(config_path, 'w') as outfile:
			json.dump(brand_new_config, outfile)

#Upload the CSV file to Azure cloud
def write_table(block_blob_service, data_container, tables_folder, table_name, table_name_suffix=''):
	"""
	write the table to blob storage.
	"""
	block_blob_service.create_blob_from_path(
	    data_container,
	    table_name + table_name_suffix + csv_suffix,
	    tables_folder + table_name + csv_suffix,
	    content_settings=ContentSettings(content_type='application/CSV')
		)

def write_new_config(block_blob_service, data_container, config_folder, table_name):
	"""
	write the config to blob storage.
	"""
	block_blob_service.create_blob_from_path(
	    data_container,
	    table_name + config_suffix,
	    config_folder + table_name + config_suffix,
	    content_settings=ContentSettings(content_type='application/CSV')
		)

def expand_table(table_name, latest_date):
	chunk_no = 0
	for data_df in pd.read_csv(in_tables_dir + table_name + csv_suffix, chunksize=5000000, parse_dates=[date_col]):
		chunk_no += 1
		print(f"Getting chunk No. {chunk_no}...")
		data_df[date_col] = data_df[date_col].dt.strftime("%Y%m%d")
		dates = list(pd.unique(data_df[date_col]))

		new_dates = [date for date in dates if date > latest_date]

		for new_date in new_dates:
			new_data_df = data_df[data_df[date_col] == new_date]
			date_suffix = suffix_delimiter + str(new_data_df[date_col].max()).replace('-','')
			table_name_and_date = table_name + date_suffix
			chunk_suffix = '_' + str(len([x for x in os.listdir(out_tables_dir) if x.startswith(table_name_and_date)]))
			new_data_df.to_csv(out_tables_dir + table_name_and_date + chunk_suffix + csv_suffix)

def concat_chunks(out_tables_dir):
	print("Concatting the chunks...")
	csv_file_list = [os.path.splitext(i)[0] for i in os.listdir(out_tables_dir) if i.endswith(csv_suffix)]
	date_suffix_set = set([x.split('-')[-1].split('_')[0] for x in csv_file_list])

	for date in date_suffix_set:
		date_table_names = [x for x in csv_file_list if date in x]

		df_list = []
		for filename in date_table_names:
		    df = pd.read_csv(out_tables_dir + filename + csv_suffix, index_col=None, header=0)
		    df_list.append(df)
		# concat the table
		concated_df = pd.concat(df_list, axis=0, ignore_index=True)
		# save concated table
		concated_df.to_csv(out_tables_dir + date_table_names[0][:-2] + csv_suffix)
		# remove chunk files from out folder
		for the_file in date_table_names:
		    file_path = os.path.join(out_tables_dir, the_file + csv_suffix)
		    try:
		        if os.path.isfile(file_path):
		            os.unlink(file_path)
		    except Exception as e:
		        print(e)

def get_new_last_date(table_name, tables_dir=out_tables_dir):
	date_suffixes = [x.split('-')[-1].split('_')[0] for x in os.listdir(out_tables_dir) if x.endswith(csv_suffix) and x.startswith(table_name)]
	max_date = max([int(s) for s in date_suffixes])

	return str(max_date)


def get_latest_date_from_config_file(file_path):
	with open(file_path) as f:
		config = json.load(f)

	return config['latest']

def update_config_file(file_path, new_last_date):
	with open(file_path) as f:
		config = json.load(f)
	config['latest'] = new_last_date

	with open(file_path, 'w') as outfile:
		json.dump(config, outfile)

def write_table_list_to_azure(block_blob_service, data_container, tables_folder, table_name_list):
	for table_name in table_name_list:
		try:
			write_table(block_blob_service, data_container, tables_folder, table_name)
			print(f'Table {table_name} sucessfuly uploaded to {data_container} storage container of BlockBlobService...')
		except Exception as e:
				print(f'Something went wrong during {table_name} table upload...')
				print(f"Exception: {str(e)}")

# TODO:
# Create the container if it does not exist.
#block_blob_service.create_container(data_container)

in_tables_list = [os.path.splitext(i)[0] for i in os.listdir(in_tables_dir) if i.endswith(csv_suffix)]
print(f"Tables to be uploaded: {in_tables_list}")
print(f'Uploading tables {in_tables_list} to {data_container} storage container of BlockBlobService...')

if not config_container:
	write_table_list_to_azure(block_blob_service, data_container, in_tables_dir, in_tables_list)
else:
	# Expand in tables & update config
	for table_name in in_tables_list:
		try:
			config_file_path = out_data_dir + table_name + config_suffix
			download_config(base_blob_service, config_container, table_name)
			latest_date = get_latest_date_from_config_file(config_file_path)
			expand_table(table_name, latest_date)
			new_last_date = get_new_last_date(table_name)
			print(f"New last date is: {new_last_date}")
			update_config_file(config_file_path, new_last_date)
			write_new_config(block_blob_service, config_container, out_data_dir, table_name)
			print(f'Config for {table_name} sucessfuly uploaded to {config_container} storage container of BlockBlobService...')
		except Exception as e:
			print(f'Something went wrong during {table_name} table upload...')
			print(f"Exception: {str(e)}")

	# Concat chunks to single date csv
	concat_chunks(out_tables_dir)

	# Get out tables list
	out_tables_list = [os.path.splitext(i)[0] for i in os.listdir(out_tables_dir) if i.endswith(csv_suffix)]
	print(f"Tables to be write: {out_tables_list}")

	# Write expanded out tables to Azure
	write_table_list_to_azure(block_blob_service, data_container, out_tables_dir, out_tables_list)

	# Remove files from out folder
	folder = out_tables_dir
	for the_file in os.listdir(folder):
	    file_path = os.path.join(folder, the_file)
	    try:
	        if os.path.isfile(file_path):
	            os.unlink(file_path)
	    except Exception as e:
	        print(e)

print('Job finished.')
