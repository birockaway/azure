# azure repo
Azure writer KBC component repo.

## Maintainer
viktor.sohajek@ecommerceholding.cz

## What it does
KBC component build based on image in dockerfile/Dockerfile uploads .csv files to specified Azure storage account. 

It has 2 modes: 
* Standard - Takes the .csv file from input mapping and uploads it to specified Azure storage account.
* Incremental - Partitions the .csv file from input mapping based on date column and 1 by 1 uploads it to specified Azure storage account with date suffix. Only files, that are older then date found in TABLE_NAME.config on azure storage account are uploaded. Date must be in format: yyyy-mm-dd.

## Minimal config - Standard
```python
{
  "account_key": "YOUR_SECRET",
  "account_name": "YOUR_ACCOUNT_NAME",
  "data_container": "folder1/folder2"
}
```

## Minimal config - Incremental
```python
{
  "account_key": "YOUR_SECRET",
  "account_name": "YOUR_ACCOUNT_NAME",
  "data_container": "folder1/folder2",
  "config_container": "folder1/folder3",
  "date_col": "YOUR_DATE_COLUMN_NAME"
}
```

For the Incremental mode to work, you need to have TABLE_NAME.config file in folder1/folder3 folder. The file must have following json in it:
```python
{
  "latest": "2019-10-14"
}
```
This will result in uploading only files for dates greater then 2019-10-14.
