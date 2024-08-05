import datetime
import hashlib
import logging
import os
import time

import polars as pl

from download_files import process_download, unzip_file, split_file
from utils import connect_preprod, pipeline_messenger, constring
from etab_clean_func import etab_file_process

cursor, db = connect_preprod()

format_str = "[%(levelname)s: %(lineno)d] %(message)s"
logging.basicConfig(level=logging.INFO, format=format_str)
logger = logging.getLogger(__name__)


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

def process_etab_fragment(filename: str) -> None:
    """
    main process to write StockEtablissement
    upserts to geo_location
    :param filename:
    :return:
    """
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

    pldf = pl.read_csv(filename, dtypes={'AddressCommuneCode': pl.Utf8,
                                         'AddressCommuneCode2': pl.Utf8,
                                         'AddressCEDEXCode': pl.Utf8,
                                         'AddressCedexCode2': pl.Utf8,
                                         'AddressNumber': pl.Utf8,
                                         'AddressNumber2': pl.Utf8,
                                         'AddressPostcode': pl.Utf8,
                                         'AddressPostcode2': pl.Utf8,
                                         'AddressPOBox': pl.Utf8,
                                         'AddressBuildingBlock': pl.Utf8,
                                         'siren': pl.Utf8}, ignore_errors=True,
                       null_values=['[ND]', 'NN'])

    # write to staging table
    t0 = time.time()
    pldf.write_database(table_name='sirene_stocketab_staging', connection_uri=constring, if_exists='append')
    t1 = time.time()
    logger.info('Sending etab file to staging in {:.2f} seconds'.format(t1 - t0))

    #  upsert to geolocation here # todo include filepath in last_modified_by
    t0 = time.time()
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
     'sirene_etab insert' as last_modified_by
     from sirene_stocketab_staging

     on duplicate key update
    address_1 = address_line_1,
    address_2 = address_line_2,
    town = AddressMunicipalityLabel,
    post_code = AddressPostcode,
    address_type = registered_office_type,
    post_code_formatted = AddressPostcode,
    date_last_modified = CURDATE(),
    last_modified_by = 'sirene_etab update'
    """)
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
    cursor.execute("""truncate table sirene_stocketab_staging""")
    db.commit()
    t1 = time.time()
    logger.info('time taken for upsert to live etab table: {}'.format(round(t1 - t0)))


def run_etab():
    current_date_month = datetime.datetime.now().month
    current_date_year = datetime.datetime.now().year
    filestring = f'{current_date_year}-{current_date_month:02d}-01-StockEtablissement_utf8.zip'

    logger.info(f'sending request with filestring: {filestring}')
    # check if zipfile is not already in the dir
    if filestring not in os.listdir():
        logger.error(f'file {filestring} does not exist or {len(os.listdir("fragments"))} is not 1')
        t0 = time.time()
        if len(os.listdir('fragments')) == 1:
            logger.info(f'no fragments found in file, downloading new file')

            # download the lastest file
            process_download(filestring=filestring)

            # unzip the file and return a .csv
            unzipped_file = unzip_file(filestring=filestring)

            # process and filter the etab csv
            clean_etab_file = etab_file_process(unzipped_file)

            # split the processed file
            split_file(unzipped_file_name=clean_etab_file)

        t1 = time.time()
        download_time = round(t1 - t0)
        logger.info(f'download and processing time: {download_time}')
    else:
        logger.info('file already uploaded OR fragments need to be processed')

    # process_download leaves the section fragments to be processed
    list_of_fragments = os.listdir('fragments')
    fragcount = 0
    try:
        t0 = time.time()
        fragment_times = []
        for fragment in list_of_fragments:
            if 'Etablissement' in filestring and 'Etablissement' in fragment:
                f_t0 = time.time()
                process_etab_fragment(filename='fragments/' + fragment)
                os.remove('fragments/' + fragment)
                fragcount += 1
                f_t1 = time.time()
                fragment_time_taken = round(f_t1 - f_t0)
                fragment_times.append(fragment_time_taken)
        t1 = time.time()
        avg_time_taken = round(sum(fragment_times) / len(fragment_times), 2)
        time_taken = t1 - t0
        pipeline_messenger(
        title= 'Sirene Stock Etablissement Pipeline has run',
        text= f'time taken: {time_taken}, average time per fragment: {avg_time_taken} seconds',
        notification_type= 'pass'
        )

    except Exception as e:
        pipeline_messenger(
            title='Sirene Stock Etablissement Pipeline has failed',
            text= f'Error in file: {filestring} - {e}',
            notification_type='fail'
        )

if __name__ == '__main__':
    run_etab()
