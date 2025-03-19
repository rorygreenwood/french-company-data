import zipfile
from filesplit.split import Split
import os
import polars as pl
from utils import zip_csv_to_parquet
from etab_batching import etab_schema, etab_file_schema


def unzip_file(filestring: str) -> str:
    # unzip the file and delete the zip file
    with zipfile.ZipFile(filestring, 'r') as zip_ref:
        zip_ref.extractall()
        infolist = zip_ref.infolist()
        if infolist:
            unzipped_file_name = infolist[0].filename
        zip_ref.close()
        return unzipped_file_name


def split_file(unzipped_file_name: str) -> None:
    """we use filesplit.split Split to divide the file into
    smaller batches of 50,000 lines"""
    split = Split(unzipped_file_name, 'fragments')
    split.bylinecount(linecount=50000, includeheader=True)

    # remove the manifest file
    os.remove('fragments/manifest')

    # once this is done, we can delete the unzipped csv


if __name__ == '__main__':
    zip_csv_to_parquet(zip_file_path='etab_test.zip',
                       parquet_name='test_etab.parquet',
                       schema=etab_file_schema)

