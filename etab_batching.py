"""
1. download file as .zip
2. open zip
3. batch_load csv
"""
import datetime
import hashlib
import os
import time
import traceback

import polars as pl
import requests

from utils import logger, connect_preprod, constring, file_cleanup, pipeline_messenger, \
    zip_csv_to_parquet

cursor, db = connect_preprod()


def create_address_line_1(input_dict: dict) -> str:
    """
    creates a concat of the columns AddressBuildingBlock, AddressNumber and AddressNumberSubUnit
    for insertion into preprod.geo_location_staging
    :param input_dict:
    :return:
    """
    # AddressBuildingBlock
    # AddressNumber
    # AddressNumberSubUnit

    output_str_list = []
    for key in input_dict.keys():
        if input_dict[key] != '' and input_dict[key] != '[ND]':
            output_str_list.append(input_dict[key])
    output_str = ' '.join(output_str_list)
    return output_str


def create_address_line_2(input_dict: dict) -> str:
    """
    creates a concat of the columns AddressUniqueIdentifier, AddressLabel
    for insertion into preprod.geo_location
    :param input_dict:
    :return:
    """
    # AddressUniqueIdentifier
    # AddressLabel
    output_str_list = []
    for key in input_dict.keys():
        if input_dict[key] != '' and input_dict[key] != '[ND]' and input_dict[key]:
            output_str_list.append(input_dict[key])
    output_str = ' '.join(output_str_list)
    return output_str


def assign_office_type(input_dict: dict) -> str:
    """
    two office types in geo_location, depending on whether True or False, it will be either Head Office or
    Sub Office
    :param input_dict:
    :return:
    """
    if input_dict['RegisteredOfficeBool']:
        return 'HEAD_OFFICE'
    elif not input_dict['RegisteredOfficeBool']:
        return 'SUB_OFFICE'


def create_org_id(input_dict: dict) -> str:
    """
    develop organisation id
    :param input_dict:
    :return:
    """
    if len(input_dict['company_number']) == 9:
        return 'FR' + str(input_dict['company_number'])
    else:
        logger.error(f'Company number {input_dict["company_number"]} not valid')
        quit()


def generate_geo_md5(input_dict: dict) -> str:
    """
    generate md5 string for unique id when inserting into geolocation

    in the case of a null postcode, we change this to empty string
    :param input_dict:
    :return:
    """
    if input_dict['AddressPostcode'] is None:
        input_dict['AddressPostcode'] = ''
    concat_str = input_dict['id'] + input_dict['AddressPostcode']
    return hashlib.md5(str(concat_str).encode('utf-8')).hexdigest()


def read_file_in_chunks(file_path, chunk_size=150000):
    df = pl.scan_parquet(file_path)
    offset = 0
    while True:
        batch = df.slice(offset=offset, length=chunk_size).collect()
        if batch.shape[0] == 0:
            break
        yield batch
        offset += chunk_size


def process_batch(df: pl.DataFrame, zipfile: str,
                  is_live: bool = True) -> None:
    unite_etab_cols = {
        'siren': 'company_number',  #
        'nic': 'localnic',  #
        'siret': 'siret',  #
        'statutDiffusionEtablissement': 'distributionStatus',
        # How publically available the company data is, O is open and P is private
        'dateCreationEtablissement': 'EstablishmentDate',  #
        'trancheEffectifsEtablissement': 'EmployeeCountCategory',  #
        'anneeEffectifsEtablissement': 'EmployeeCountCategoryYear',  #
        'activitePrincipaleRegistreMetiersEtablissement': 'mainNAF',  #
        'dateDernierTraitementEtablissement': 'LastNAFUpdate',  #
        'etablissementSiege': 'RegisteredOfficeBool',  # either True or False
        'nombrePeriodesEtablissement': 'PeriodNumber',
        'dernierNumeroVoieEtablissement': 'LastAddressNumber',
        'indiceRepetitionDernierNumeroVoieEtablissement': 'DateOfLastAddressNumber',

        'identifiantAdresseEtablissement': 'InstitutionAddressID',
        'coordonneeLambertAbscisseEtablissement': 'LambertCoordinateX',
        'coordonneeLambertOrdonneeEtablissement': 'LambertCoordinateY',

        # details number of periods the establishment has been written as office
        'complementAdresseEtablissement': 'AddressBuildingBlock',  #
        'numeroVoieEtablissement': 'AddressNumber',  # -12-b Example Way
        'indiceRepetitionEtablissement': 'AddressNumberSubUnit',  # 12-b- Example Way
        'typeVoieEtablissement': 'AddressUniqueIdentifier',  #
        'libelleVoieEtablissement': 'AddressLabel',  #
        'codePostalEtablissement': 'AddressPostcode',  #
        'libelleCommuneEtablissement': 'AddressMunicipalityLabel',  #
        'libelleCommuneEtrangerEtablissement': 'AddressForeignMunicipality',  # only if foreign address
        'distributionSpecialeEtablissement': 'AddressPOBox',  #
        'codeCommuneEtablissement': 'AddressCommuneCode',  #
        'codeCedexEtablissement': 'AddressCEDEXCode',  #
        'libelleCedexEtablissement': 'AddressCEDEXLabel',  #
        'codePaysEtrangerEtablissement': 'AddressOverseasCountryCode',  #
        'libellePaysEtrangerEtablissement': 'AddressOverseasCountryLabel',  #
        'complementAdresse2Etablissement': 'AddressBuildingBlock2',  #
        'numeroVoie2Etablissement': 'AddressNumber2',  #
        'indiceRepetition2Etablissement': 'AddressNumberSubUnit2',  #
        'typeVoie2Etablissement': 'AddressUniqueIdentifier2',  #
        'libelleVoie2Etablissement': 'AddressLabel2',  #
        'codePostal2Etablissement': 'AddressPostcode2',  #
        'libelleCommune2Etablissement': 'AddressMunicipalityLabel2',  #
        'libelleCommuneEtranger2Etablissement': 'AddressForeignMunicipality2',  #
        'distributionSpeciale2Etablissement': 'AddressPOBox2',  #
        'codeCommune2Etablissement': 'AddressCommuneCode2',  #
        'codeCedex2Etablissement': 'AddressCEDEXCode2',  #
        'libelleCedex2Etablissement': 'AddressCEDEXLabel2',  #
        'codePaysEtranger2Etablissement': 'AddressOverseasCountryCode2',  #
        'libellePaysEtranger2Etablissement': 'AddressOverseasCountryLabel2',  #
        'dateDebut': 'DateOfBusinessStart',  #
        'etatAdministratifEtablissement': 'AdministrativeStatus',  # A for active, F for closed
        'enseigne1Etablissement': 'EstablishmentSign1',  #
        'enseigne2Etablissement': 'EstablishmentSign2',  #
        'enseigne3Etablissement': 'EstablishmentSign3',  #
        'denominationUsuelleEtablissement': 'CommonCompanyName',  # company publicly known as
        'activitePrincipaleEtablissement': 'APETCode',  #
        'nomenclatureActivitePrincipaleEtablissement': 'APETCodeCategory',  #
        'caractereEmployeurEtablissement': 'EmploymentType',  #
    }

    t0 = time.time()
    df = df.rename(unite_etab_cols)
    df = df.fill_null('')
    df = df.fill_nan('')
    df = df.with_columns(
        pl.struct(['company_number']).map_elements(create_org_id, return_dtype=pl.Utf8).alias('id'))

    # get original size for analytics
    original_df_size = len(df)

    # todo remove closed addresses
    logger.debug(f'size of file before removing closed offices for current dataframe: {len(df)}')
    df = df.filter(pl.col('AdministrativeStatus') != 'F')
    logger.debug(f'size of file after removing closed offices for current dataframe: {len(df)}')

    # generate md5 hash
    df = df.with_columns(
        pl.struct(['id', 'AddressPostcode']).map_elements(generate_geo_md5, return_dtype=pl.Utf8).alias('geo_md5'))

    # create first line of address
    # todo exceptions.ComputeError: TypeError: sequence item 0: expected str instance, NoneType found

    df = df.with_columns(
        pl.struct(['AddressBuildingBlock', 'AddressNumber', 'AddressNumberSubUnit']).map_elements(create_address_line_1,
                                                                                                  return_dtype=pl.Utf8).alias(
            'address_line_1'))

    # create second line of address
    df = df.with_columns(pl.struct(
        ['AddressUniqueIdentifier', 'AddressLabel']).map_elements(
        create_address_line_2, return_dtype=pl.Utf8).alias('address_line_2'))

    # determine whether the office is a head office or no
    df = df.with_columns(
        pl.struct(['RegisteredOfficeBool']).map_elements(assign_office_type, return_dtype=pl.Utf8).alias(
            'registered_office_type'))

    # for diagnostic purposes, add filenames and update times into the dataframe
    df = df.with_columns(pl.lit(zipfile).alias('last_modified_by'))
    df = df.with_columns(pl.lit(datetime.datetime.now()).alias('last_modified_date'))

    cursor.execute("""truncate table sirene_stocketab_staging""")
    db.commit()
    logger.info('writing to db')
    df.write_database(table_name='sirene_stocketab_staging', connection=constring, if_table_exists='append')
    if is_live:
        cursor.execute("""
        insert ignore into geo_location (
        address_1, 
        address_2, 
        town,  
        country, 
        post_code, 
        address_type,  
        organisation_id, 
        post_code_formatted, 
        md5_key, 
        date_last_modified, 
        last_modified_by) 
    
        select 
         address_line_1 as address_1,
         address_line_2 as address_2,
         AddressMunicipalityLabel as town, 
         'France' as country,
         AddressPostcode as post_code,
         registered_office_type as address_type,
         id as organisation_id,
         AddressPostcode as post_code_formatted,
         geo_md5 as md5_key,
         curdate() as date_last_modified,
         %s as last_modified_by
         from sirene_stocketab_staging
    
         on duplicate key update
        address_1 = address_line_1,
        address_2 = address_line_2,
        town = AddressMunicipalityLabel,
        post_code = AddressPostcode,
        address_type = registered_office_type,
        post_code_formatted = AddressPostcode,
        date_last_modified = CURDATE(),
        last_modified_by = %s
        """, (zipfile, zipfile))
        db.commit()
        t1 = time.time()
        logger.info('time taken for upsert to geo_location: {}'.format(round(t1 - t0)))

        # upsert into larger stock etab table for debugging when needed, similar to rchis
        t0 = time.time()
        cursor.execute(
            """
        insert into sirene_stocketab
        select * from sirene_stocketab_staging t2
        on duplicate key update
        sirene_stocketab.company_number = t2.company_number,
        sirene_stocketab.localnic = t2.localnic,
        sirene_stocketab.siret = t2.siret,
        sirene_stocketab.distributionStatus = t2.distributionStatus,
        sirene_stocketab.EstablishmentDate = t2.EstablishmentDate,
        sirene_stocketab.EmployeeCountCategory = t2.EmployeeCountCategory,
        sirene_stocketab.EmployeeCountCategoryYear = t2.EmployeeCountCategoryYear,
        sirene_stocketab.mainNAF = t2.mainNAF,
        sirene_stocketab.LastNAFUpdate = t2.LastNAFUpdate,
        sirene_stocketab.RegisteredOfficeBool = t2.RegisteredOfficeBool,
        sirene_stocketab.PeriodNumber = t2.PeriodNumber,
        sirene_stocketab.AddressBuildingBlock = t2.AddressBuildingBlock,
        sirene_stocketab.AddressNumber = t2.AddressNumber,
        sirene_stocketab.AddressNumberSubUnit = t2.AddressNumberSubUnit,
        sirene_stocketab.AddressUniqueIdentifier = t2.AddressUniqueIdentifier,
        sirene_stocketab.AddressLabel = t2.AddressLabel,
        sirene_stocketab.AddressPostcode = t2.AddressPostcode,
        sirene_stocketab.AddressMunicipalityLabel = t2.AddressMunicipalityLabel,
        sirene_stocketab.AddressForeignMunicipality = t2.AddressForeignMunicipality,
        sirene_stocketab.AddressPOBox = t2.AddressPOBox,
        sirene_stocketab.AddressCommuneCode = t2.AddressCommuneCode,
        sirene_stocketab.AddressCEDEXCode = t2.AddressCEDEXCode,
        sirene_stocketab.AddressCEDEXLabel = t2.AddressCEDEXLabel,
        sirene_stocketab.AddressOverseasCountryCode = t2.AddressOverseasCountryCode,
        sirene_stocketab.AddressOverseasCountryLabel = t2.AddressOverseasCountryLabel,
        sirene_stocketab.AddressBuildingBlock2 = t2.AddressBuildingBlock2,
        sirene_stocketab.AddressNumber2 = t2.AddressNumber2,
        sirene_stocketab.AddressNumberSubUnit2 = t2.AddressNumberSubUnit2,
        sirene_stocketab.AddressUniqueIdentifier2 = t2.AddressUniqueIdentifier2,
        sirene_stocketab.AddressLabel2 = t2.AddressLabel2,
        sirene_stocketab.AddressPostcode2 = t2.AddressPostcode2,
        sirene_stocketab.AddressMunicipalityLabel2 = t2.AddressMunicipalityLabel2,
        sirene_stocketab.AddressForeignMunicipality2 = t2.AddressForeignMunicipality2,
        sirene_stocketab.AddressPOBox2 = t2.AddressPOBox2,
        sirene_stocketab.AddressCommuneCode2 = t2.AddressCommuneCode2,
        sirene_stocketab.AddressCEDEXCode2 = t2.AddressCEDEXCode2,
        sirene_stocketab.AddressCEDEXLabel2 = t2.AddressCEDEXLabel2,
        sirene_stocketab.AddressOverseasCountryCode2 = t2.AddressOverseasCountryCode2,
        sirene_stocketab.AddressOverseasCountryLabel2 = t2.AddressOverseasCountryLabel2,
        sirene_stocketab.DateOfBusinessStart = t2.DateOfBusinessStart,
        sirene_stocketab.AdministrativeStatus = t2.AdministrativeStatus,
        sirene_stocketab.EstablishmentSign1 = t2.EstablishmentSign1,
        sirene_stocketab.EstablishmentSign2 = t2.EstablishmentSign2,
        sirene_stocketab.EstablishmentSign3 = t2.EstablishmentSign3,
        sirene_stocketab.CommonCompanyName = t2.CommonCompanyName,
        sirene_stocketab.APETCode = t2.APETCode,
        sirene_stocketab.APETCodeCategory = t2.APETCodeCategory,
        sirene_stocketab.EmploymentType = t2.EmploymentType,
        sirene_stocketab.geo_md5 = t2.geo_md5,
        sirene_stocketab.last_modified_date = t2.last_modified_date,
        sirene_stocketab.last_modified_by = t2.last_modified_by
    
            """
        )
        db.commit()

        t1 = time.time()
        logger.info('time taken for upsert to live etab table: {}'.format(round(t1 - t0)))


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


etab_schema = {'AddressCommuneCode': pl.Utf8,
               'AddressCommuneCode2': pl.Utf8,
               'AddressCEDEXCode': pl.Utf8,
               'AddressCedexCode2': pl.Utf8,
               'AddressNumber': pl.Utf8,
               'AddressNumber2': pl.Utf8,
               'AddressPostcode': pl.Utf8,
               'AddressPostcode2': pl.Utf8,
               'AddressPOBox': pl.Utf8,
               'AddressBuildingBlock': pl.Utf8,
               'codeCommuneEtablissement': pl.Utf8,
               'numeroVoieEtablissement': pl.Utf8,
               'codeCedexEtablissement': pl.Utf8,
               'codePostalEtablissement': pl.Utf8,
               'numeroVoie2Etablissement': pl.Utf8,
               'codePostal2Etablissement': pl.Utf8,
               'distributionSpecialeEtablissement': pl.Utf8,
               'complementAdresseEtablissement': pl.Utf8,
               'siren': pl.Utf8}

etab_file_schema = {'codeCommuneEtablissement': pl.Utf8,
                    'codeCedexEtablissement': pl.Utf8,
                    'numeroVoieEtablissement': pl.Utf8,
                    'codePostalEtablissement': pl.Utf8,
                    'numeroVoie2Etablissement': pl.Utf8,
                    'codePostal2Etablissement': pl.Utf8,
                    'distributionSpecialeEtablissement': pl.Utf8,
                    'complementAdresseEtablissement': pl.Utf8,
                    'coordonneeLambertAbscisseEtablissement': pl.Utf8,
                    'coordonneeLambertOrdonneeEtablissement': pl.Utf8,
                    'siren': pl.Utf8}


def verify_pipeline(file_length: int, file_name: str) -> bool:
    cursor.execute("""select * from sirene_stocketab where last_modified_by = %s""", (file_name,))
    res = cursor.fetchall()[0][0]
    if res == file_length:
        return True
    else:
        pipeline_messenger(
            title='French Legal Flag',
            text=f'{res} / {file_length} records have been properly loaded.',
            notification_type='notification'
        )
        return False


def main() -> None:
    current_date_month = datetime.datetime.now().month
    current_date_year = datetime.datetime.now().year
    filestring = f'{current_date_year}-{current_date_month:02d}-01-StockEtablissement_utf8.zip'
    csv_file = 'StockEtablissement_utf8.csv'
    logger.info(f'sending request with filestring: {filestring}')
    parquet_file = 'StockEtablissement_utf8.parquet'
    t0 = time.time()
    if filestring not in os.listdir():
        zipped_file = process_download(filestring)
    else:
        zipped_file = filestring

    if 'StockEtablissement_utf8.parquet' not in os.listdir():
        file_size = zip_csv_to_parquet(zip_file_path=zipped_file,
                                       parquet_name=parquet_file,
                                       schema=etab_file_schema)
    chunk_size = 150000

    for df in read_file_in_chunks(parquet_file):
        try:
            process_batch(df=df, zipfile=zipped_file)
        except Exception as e:
            logger.error(e)
            df.write_csv('error.csv')

            pipeline_messenger(
                title='French Etab Fail',
                text=f'{e} - {traceback.format_exc()}',
                notification_type='fail'
            )

            file_list = [zipped_file, csv_file]
            file_cleanup(file_list=file_list)
            quit()

    t1 = time.time()

    pipeline_messenger(
        title='French Etab Pass',
        text=f'{t1 - t0}',
        notification_type='pass'
    )
    verify_pipeline(file_length=file_size, file_name=zipped_file)
    file_list = [zipped_file, csv_file]
    file_cleanup(file_list=file_list)


if __name__ == '__main__':
    main()
