import time
import re
import os

import polars as pl
import requests
import zipfile

from utils import logger, constring, connect_preprod

cursor, db = connect_preprod()

unite_legale_cols = {
    # breakdowns of each column name can be found on https://www.sirene.fr/static-resources/htm/v_sommaire_311.htm#7
    'siren': 'company_number',  # we know this one
    'etatAdministratifUniteLegale': 'AdministrativeStatus',  # A means active, C means inactive
    'categorieJuridiqueUniteLegale': 'LegalCategory',
    'denominationUniteLegale': 'LegalEntityName',
    'nomenclatureActivitePrincipaleUniteLegale': 'ActiveLegalUnit',
    'activitePrincipaleUniteLegale': 'NAFCategory',  # different naf based on when the company set up
}


def process_download(filestring: str) -> str:
    """check for file, if not exists, download, unzip and split"""

    # build the url for the request, by appending filestring var to files_url
    files_url = 'https://files.data.gouv.fr/insee-sirene/'
    request_url = files_url + filestring

    # check for
    list_of_files = os.listdir()
    downloaded = 0
    for file in list_of_files:
        if '.zip' in file and file == filestring:
            logger.info('{} has been found'.format(filestring))
            downloaded = 1

    # if a zip file has not been found, download one
    if downloaded == 0:

        # send a request to recieve the file
        r = requests.get(request_url, stream=True, verify=False)

        # if we recieve a 200, that files exists and we can continue
        if r.status_code != 200:
            logger.error('status code: {}'.format(r.status_code))
            raise requests.exceptions.HTTPError

        # create a new file, and write in the data from the request
        with open(filestring, 'wb') as f:
            chunkcount = 0
            for chunk in r.iter_content(chunk_size=50000):
                chunkcount += 1

                f.write(chunk)
                if chunkcount % 100 == 0:
                    logger.info(chunkcount)

            logger.info('file successfully downloaded')
    return filestring


def zip_csv_to_parquet(zip_file_path: str,
                       csv_name: str = 'StockUniteLegale_utf8.csv',
                       parquet_name: str = 'StockUniteLegale_utf8.parquet') -> None:
    """
    Convert a .csv file contained with a .zip file into a .parquet file, to further improve storage capability.

    :param zip_file_path:
    :param csv_name:
    :param parquet_name:
    :return:
    """
    with zipfile.ZipFile(zip_file_path, 'r') as zip_obj:
        with zip_obj.open(csv_name) as csv_file:
            df = pl.read_csv(csv_file,
                             columns=list(unite_legale_cols.keys()),
                             schema_overrides={k: pl.Utf8 for k in unite_legale_cols.keys()})
            df.write_parquet(parquet_name, compression='gzip')


def read_file_in_chunks(file_path, chunk_size=150000):
    """
    Scan paruquet and use an incremental offset value to gradually sift through the rows, rather than
    unzipping and splitting the file into smaller sections to parse over time.

    todo use the offset value in database forensics - if there is a dodgy record, which offset range could it be
        found in?
    :param file_path:
    :param chunk_size:
    :return:
    """

    df = pl.scan_parquet(file_path)

    df = df.rename(unite_legale_cols)
    # check counts before filtering
    naf_counts_test = df.filter(pl.col('NAFCategory') == '68.20B')

    # filter for active entities
    df = df.filter(pl.col('AdministrativeStatus') == 'A')

    # filter out dodgy names
    df = df.drop_nulls(subset='LegalEntityName')

    df = df.filter(pl.col('LegalEntityName') != '[ND]')

    # filter for companies only (0000 means it is a person)
    df = df.filter(pl.col('LegalCategory') != '0000')

    # filter for private companies (5---)
    df = df.filter(pl.col('LegalCategory').str.slice(0, 1) == '5')

    # filter for the current naf code revision
    df = df.filter(pl.col('ActiveLegalUnit') == 'NAFRev2')
    offset = 0
    while True:
        batch = df.slice(offset=offset, length=chunk_size).select(list(unite_legale_cols.values())).collect()
        if batch.shape[0] == 0:
            break
        yield batch
        offset += chunk_size


def main(filename: str) -> None:
    # todo error handling on batch loading
    # filename = '2025-01-01-StockUniteLegale_utf8.zip'
    filename_csv = 'StockUniteLegale_utf8.csv'
    filename_parquet = 'StockUniteLegale_utf8.parquet'
    date_of_file = re.findall(string=filename, pattern='[0-9]{4}-[0-9]{2}-[0-9]{2}')[0]

    naf_code_df: pl.DataFrame = pl.read_csv('_naf_code_data.csv', schema_overrides={'iSIC_code': pl.Utf8})

    if filename not in os.listdir():
        file = process_download(filename)

    if filename_csv not in os.listdir():
        zip_csv_to_parquet(filename)

    naf_df = pl.DataFrame()
    for df in read_file_in_chunks(filename_parquet):
        # df = df.rename({'NAFCategory': 'naf'})
        naf_df = pl.concat([naf_df, df], how='vertical')

    counts_df = naf_df['NAFCategory'].value_counts()

    naf_counts_join = counts_df.join(naf_code_df, how='left', left_on='NAFCategory', right_on='naf_code')
    naf_counts_join = naf_counts_join.with_columns(file_date=pl.lit(date_of_file))

    cursor.execute("""truncate table naf_code_counts_staging""")
    db.commit()

    naf_counts_join.write_database(
        table_name='naf_code_counts_staging',
        connection=constring,
        if_table_exists='append'
    )

    # insert from staging into live tables
    cursor.execute("""
    insert into naf_code_counts 
        (NAFCategory, 
        count, 
        description, 
        iSIC_code,
        naf_code_counts.file_date, 
        md5_str, 
        isic_md5, 
        last_modified_by, 
        last_modified_date) 
    select 
        NAFCategory,
        count, 
        description, 
        iSIC_code, 
        naf_code_counts_staging.file_date, 
        md5(concat(file_date, NAFCategory)) as file_date, 
        md5(concat(naf_code_counts_staging.file_date, iSIC_code)), 
        'insert', 
        now()
    from naf_code_counts_staging
    on duplicate key update
        naf_code_counts.iSIC_code = naf_code_counts_staging.iSIC_code,
        naf_code_counts.count = naf_code_counts_staging.count,
        naf_code_counts.last_modified_date = now(),
        naf_code_counts.last_modified_by = 'naf update with filter?'
     """)
    db.commit()

    # insert into isic tables
    cursor.execute("""
    insert into isic_code_counts 
        (month, 
        isic_code, 
        naf_code, 
        naf_count, 
        md5_str,
        last_modified_date, 
        last_modified_by)
    select 
        file_date, 
        iSIC_code, 
        NAFCategory, 
        count, 
        md5(concat(file_date, iSIC_code)), 
        now(), 
        'naf_code_insert' 
    from naf_code_counts
    where iSIC_code is not null and iSIC_code <> '' and md5(concat(file_date, iSIC_code)) is not null and md5(concat(file_date, iSIC_code)) <> ''
    on duplicate key update 
        naf_code = NAFCategory,
        naf_count = naf_code_counts.count,
        isic_code_counts.isic_count = naf_code_counts.count + isic_code_counts.naf_count,
        last_modified_date = NOW(),
        last_modified_by = 'insert test fr 22/08 update?' 
    """)
    db.commit()


if __name__ == '__main__':
    for i in range(1, 13):
        filestring = f'2022-{i:02d}-01-StockUniteLegale_utf8.zip'
        t0 = time.time()
        main(filestring)
        t1 = time.time()
        print(f'time taken: {t1 - t0}')
