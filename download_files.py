import requests

import re
import zipfile
import os
from filesplit.split import Split
import time
import datetime
import polars as pl
import hashlib
import logging

from utils import connect_preprod

cursor, db = connect_preprod()

constring = f'mysql://{os.environ.get("ADMINUSER")}:{os.environ.get("ADMINPASS")}@{os.environ.get("PREPRODHOST")}:3306/{os.environ.get("DATABASE")}'

format_str = "[%(levelname)s: %(lineno)d] %(message)s"
logging.basicConfig(level=logging.INFO, format=format_str)
logger = logging.getLogger(__name__)

def map_employee_count(input_dict: dict) -> str:
    tranche_effectis_dict = {  # dictionary of what each number means in terms of workers
        '01': '1-2 employees',
        '02': '2-3 employees',
        '03': '6-9 employees',
        '11': '10-19 employees',
        '12': '20-49 employees',
        '21': '50-99 employees',
        '22': '100-199 employees',
        '31': '200-249 employees',
        '32': '250-499 employees',
        '41': '500-999 employees',
        '42': '1000-1999 employees',
        '51': '2000-4999 employees',
        '52': '5000-9999 employees',
        '53': '>10000 employees',
        'null': 'no number provided',
    }
    return tranche_effectis_dict[input_dict['EmployeeCountCategory']]

def create_org_id(input_dict: dict) -> str:
    return 'FR' + str(input_dict['company_number'])

def generate_geo_md5(input_dict: dict) -> str:
    if input_dict['AddressPostcode'] is None:
        input_dict['AddressPostcode'] = ''
    concat_str ='l'+ input_dict['AddressPostcode']
    return hashlib.md5(str(concat_str).encode('utf-8')).hexdigest()

def process_legal_fragment(filename: str) -> None:
    unite_legale_cols = {
        # breakdowns of each column name can be found on https://www.sirene.fr/static-resources/htm/v_sommaire_311.htm#7
        'siren': 'company_number',  # we know this one
        'statutDiffusionUniteLegale': 'LegalUnitBroadcastID',  # Dissemination status of the legal unit.
        'unitePurgeeUniteLegale': 'PurgeStatus',  # whether or not the legal unit has been purged (removed?)
        'dateCreationUniteLegale': 'DateCreated',  # date the
        'sigleUniteLegale': 'LegalAcronym',  # legal acronym?
        'sexeUniteLegale': 'GenderOfPerson',  # person/company's gender?
        'prenom1UniteLegale': 'NaturalName1',  # not applicable to legal entities
        'prenom2UniteLegale': 'NaturalName2',  # not applicable to legal entities
        'prenom3UniteLegale': 'NaturalName3',  # not applicable to legal entities
        'prenom4UniteLegale': 'NaturalName4',  # not applicable to legal entities
        'prenomUsuelUniteLegale': 'PreferredName',  # not applicable to legal entities
        'pseudonymeUniteLegale': 'pseudonym',  # pseudonym of the natural person
        'identifiantAssociationUniteLegale': 'RNANumber',  #
        'trancheEffectifsUniteLegale': 'EmployeeCountCategory',
        'anneeEffectifsUniteLegale': 'EmployeeCountCategoryDateUpdated',
        # year when the employee number was last recorded
        'dateDernierTraitementUniteLegale': 'LegalUnitUpdated',  #
        'nombrePeriodesUniteLegale': 'TimeAsLegalUnit',  #
        'categorieEntreprise': 'BusinessCategory',  # either SME (small-medium enterprise), Medium or GE (Large)
        'anneeCategorieEntreprise': 'YearOfBusinessCategoryAssignment',  #
        'dateDebut': 'DateOfBusinessStart',  #
        'etatAdministratifUniteLegale': 'AdministrativeStatus',  #
        'nomUniteLegale': 'PersonBirthName',  # not applicable
        'nomUsageUniteLegale': 'PersonUsedName',  # not applicable
        'denominationUniteLegale': 'LegalEntityName',  # company name
        'denominationUsuelle1UniteLegale': 'LegalEntityName1',  # company name
        'denominationUsuelle2UniteLegale': 'LegalEntityName2',  # company name
        'denominationUsuelle3UniteLegale': 'LegalEntityName3',  # company name
        'categorieJuridiqueUniteLegale': 'LegalCategory',  #
        'activitePrincipaleUniteLegale': 'NAFCategory',  # different naf based on when the company set up
        'nomenclatureActivitePrincipaleUniteLegale': 'ActiveLegalUnit',  #
        'nicSiegeUniteLegale': 'NICAssignment',  #
        'economieSocialeSolidaireUniteLegale': 'SSEBool',  #
        'societeMissionUniteLegale': 'MissionDrivenCompanyBool',  #
        'caractereEmployeurUniteLegale': 'EmployerNature',  # largely null according to sirene
    }

    # prepare the stock legal file for insert into staging
    t0 = time.time()
    pldf = pl.read_csv(filename, dtypes={'codeCommuneEtablissement': pl.Utf8,
                                         'siren': pl.Utf8,
                                         'siret': pl.Utf8}, ignore_errors=True)

    pldf = pldf.rename(unite_legale_cols)
    pldf.drop_nulls(subset='LegalEntityName')
    pldf = pldf.filter(pl.col('LegalEntityName') != '[ND]')
    pldf = pldf.with_columns(pl.struct(['company_number']).apply(create_org_id, return_dtype=pl.Utf8).alias('id'))
    pldf = pldf.with_columns(country=pl.lit('FRANCE'),
                      country_code=pl.lit('FR'),
                      last_modified_by=pl.lit('sirene_stock_legale upload'),
                      date_last_modified=pl.lit(datetime.date.today().strftime('%m/%d/%Y')),)

    # pldf_employee_count_column = (pl.col('EmployeeCountCategory').apply(map_employee_count).alias('EmployeeCountCategory'))
    # pldf = pldf.with_columns(pldf_employee_count_column)
    t1 = time.time()
    logger.info('time taken to prepare stock legal: {}'.format(round(t1 - t0)))

    # sending polars dataframe to staging table
    t0 = time.time()
    pldf.write_database(table_name='sirene_stocklegal_staging',
                        connection_uri=constring, if_exists='replace',
                        )
    t1 = time.time()

    logger.info('time taken to write stock legal into staging: {}'.format(round(t1 - t0)))
    # todo upsert into organisation from the staging table here, and then insert into stock_legal
    # cursor.execute(
    #     """
    #     insert into organisation (
    # id,
    # company_number,
    # company_name,
    # is_active,
    # company_status,
    # country,
    # date_formed,
    # company_type,
    # last_modified_by,
    # last_modified_date,
    # country_code)
    # select
    # id,
    # company_number,
    # LegalEntityName,
    # TRUE,
    # TRUE,
    # country,
    # DateCreated,
    # BusinessCategory,
    # 'stock_legal_update_org.py INSERT' as last_modified_by,
    # curdate() as last_modified_date,
    # 'FR' as country_code
    # from sirene_stocklegal_staging
    #
    # on duplicate key update
    # company_name = LegalEntityName,
    # last_modified_by = 'stock_legal_update_org.py UPDATE',
    # last_modified_date = curdate()"""
    # )
    # db.commit()
    t0 = time.time()

    # insert naf code data into NAF code
    cursor.execute(
        """insert into naf_code_staging (code, organisation_id, name_en, name_fr, last_modified_date, last_modified_by) 
        select id, NAFCategory, t2.name_en, t2.name_fr, curdate(), 'stock_legal pipeline' from sirene_stocklegal t1
inner join naf_codes_translations t2
on t1.NAFCategory = t2.code
        """
    )
    db.commit()

    t1 = time.time()
    logger.info('time taken to insert NAF codes into staging: {}'.format(round(t1 - t0)))

    # upsert staging table into live table
    t0 = time.time()
    cursor.execute(
        """
        insert into sirene_stocklegal
        select * from sirene_stocklegal_staging t2
        on duplicate key update 
            sirene_stocklegal.company_number = t2.company_number,
    sirene_stocklegal.LegalUnitBroadcastID = t2.LegalUnitBroadcastID,
    sirene_stocklegal.PurgeStatus = t2.PurgeStatus,
    sirene_stocklegal.DateCreated = t2.DateCreated,
    sirene_stocklegal.LegalAcronym = t2.LegalAcronym,
    sirene_stocklegal.GenderOfPerson = t2.GenderOfPerson,
    sirene_stocklegal.NaturalName1 = t2.NaturalName1,
    sirene_stocklegal.NaturalName2 = t2.NaturalName2,
    sirene_stocklegal.NaturalName3 = t2.NaturalName3,
    sirene_stocklegal.NaturalName4 = t2.NaturalName4,
    sirene_stocklegal.PreferredName = t2.PreferredName,
    sirene_stocklegal.pseudonym = t2.pseudonym,
    sirene_stocklegal.RNANumber = t2.RNANumber,
    sirene_stocklegal.EmployeeCountCategory = t2.EmployeeCountCategory,
    sirene_stocklegal.EmployeeCountCategoryDateUpdated = t2.EmployeeCountCategoryDateUpdated,
    sirene_stocklegal.LegalUnitUpdated = t2.LegalUnitUpdated,
    sirene_stocklegal.TimeAsLegalUnit = t2.TimeAsLegalUnit,
    sirene_stocklegal.BusinessCategory = t2.BusinessCategory,
    sirene_stocklegal.YearOfBusinessCategoryAssignment = t2.YearOfBusinessCategoryAssignment,
    sirene_stocklegal.DateOfBusinessStart = t2.DateOfBusinessStart,
    sirene_stocklegal.AdministrativeStatus = t2.AdministrativeStatus,
    sirene_stocklegal.PersonBirthName = t2.PersonBirthName,
    sirene_stocklegal.PersonUsedName = t2.PersonUsedName,
    sirene_stocklegal.LegalEntityName = t2.LegalEntityName,
    sirene_stocklegal.LegalEntityName1 = t2.LegalEntityName1,
    sirene_stocklegal.LegalEntityName2 = t2.LegalEntityName2,
    sirene_stocklegal.LegalEntityName3 = t2.LegalEntityName3,
    sirene_stocklegal.LegalCategory = t2.LegalCategory,
    sirene_stocklegal.NAFCategory = t2.NAFCategory,
    sirene_stocklegal.ActiveLegalUnit = t2.ActiveLegalUnit,
    sirene_stocklegal.NICAssignment = t2.NICAssignment,
    sirene_stocklegal.SSEBool = t2.SSEBool,
    sirene_stocklegal.MissionDrivenCompanyBool = t2.MissionDrivenCompanyBool,
    sirene_stocklegal.EmployerNature = t2.EmployerNature,
    sirene_stocklegal.id = t2.id,
    sirene_stocklegal.country = t2.country,
    sirene_stocklegal.country_code = t2.country_code,
    sirene_stocklegal.last_modified_by = 'testing leading 0s',
    sirene_stocklegal.date_last_modified = t2.date_last_modified
        """
    )
    db.commit()
    cursor.execute("""truncate table sirene_stocklegal_staging""")
    db.commit()
    t1 = time.time()
    logger.info('time taken to upsert into live tables: {}'.format(round(t1-t0)))


def create_address_line(input_dict: dict) -> str:
    # AddressBuildingBlock
    # AddressNumber
    # AddressSubUnit

    output_str_list = []
    for key in input_dict.keys():
        if input_dict[key] != '':
            output_str_list.append(input_dict[key])
    output_str = ' '.join(output_str_list)
    return output_str
def create_address_line(input_dict: dict) -> str:
    # AddressUniqueIdentifier
    # AddressLabel
    output_str_list = []
    for key in input_dict.keys():
        if input_dict[key] != '':
            output_str_list.append(input_dict[key])
    output_str = ' '.join(output_str_list)
    return output_str
def assign_office_type(input_dict: dict) -> str:
    if input_dict['RegisteredOfficeBool']:
        return 'HEAD_OFFICE'
    elif not input_dict['RegisteredOfficeBool']:
        return 'SUB_OFFICE'
def process_etab_fragment(filename: str) -> None:
    unite_etab_cols = {
        'siren': 'company_number',  #
        'nic': 'localnic',  #
        'siret': 'siret',  #
        'statutDiffusionEtablissement': 'distributionStatus',  # How publically available the company data is, O is open and P is private
        'dateCreationEtablissement': 'EstablishmentDate',  #
        'trancheEffectifsEtablissement': 'EmployeeCountCategory',  #
        'anneeEffectifsEtablissement': 'EmployeeCountCategoryYear',  #
        'activitePrincipaleRegistreMetiersEtablissement': 'mainNAF',  #
        'dateDernierTraitementEtablissement': 'LastNAFUpdate',  #
        'etablissementSiege': 'RegisteredOfficeBool',  # either True or False
        'nombrePeriodesEtablissement': 'PeriodNumber',
        'dernierNumeroVoieEtablissement': 'LastAddressNumber',
        'indiceRepetitionDernierNumeroVoieEtablissement': 'DateOfLastAddressNumber',

        'identifiantAdresseEtablissement': 'InstitutionAddressIdentifier',
        'coordonneeLambertAbscisseEtablissement': 'LambertCoordinationX',
        'coordonneeLambertOrdonneeEtablissement': 'LambertCoordinationY',

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
    pldf = pl.read_csv(filename, dtypes={'codeCommuneEtablissement': pl.Utf8,
                                         'codeCedexEtablissement': pl.Utf8,
                                         'numeroVoieEtablissement': pl.Utf8,
                                         'codePostalEtablissement': pl.Utf8,
                                         'numeroVoie2Etablissement': pl.Utf8,
                                         'codePostal2Etablissement': pl.Utf8,
                                         'distributionSpecialeEtablissement': pl.Utf8}, ignore_errors=True)
    pldf = pldf.rename(unite_etab_cols)
    pldf = pldf.fill_null('')
    pldf = pldf.with_columns(pl.struct(['company_number']).apply(create_org_id, return_dtype=pl.Utf8).alias('id'))
    pldf = pldf.with_columns(pl.struct(['id', 'AddressPostcode']).apply(generate_geo_md5, return_dtype=pl.Utf8).alias('geo_md5'))
    t1 = time.time()
    logger.info('Preparing etab file in {:.2f} seconds'.format(t1 - t0))
    pldf = pldf.with_columns(pl.struct(['AddressBuildingBlock', 'AddressNumber', 'AddressSubUnit']).apply(create_address_line).alias('address_line_1'))
    pldf = pldf.with_columns(pl.struct(
        ['AddressUniqueIdentifier', 'AddressLabel']).apply(
        create_address_line).alias('address_line_2'))
    pldf = pldf.with_columns(pl.struct(['RegisteredOfficeBool']).apply(assign_office_type).alias('registered_office_type'))
    t0 = time.time()
    pldf.write_database(table_name='sirene_stocketab_staging', connection_uri=constring, if_exists='append')
    t1 = time.time()
    logger.info('Sending etab file to staging in {:.2f} seconds'.format(t1 - t0))
    # todo upsert to geolocation here
    t0 = time.time()
    cursor.execute("""
    insert into geo_location (
    address_1, 
    address_2, 
    town,  
    country, 
    post_code, 
    address_type,  
    latitude, 
    longitude, 
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
     LambertCoordinateX as latitude,
     LambertCoordinateY as longitude,
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
    latitude = LambertCoordinateX,
    longitude = LambertCoordinateY,
    post_code_formatted = AddressPostcode,
    date_last_modified = CURDATE(),
    last_modified_by = 'sirene_etab update'
    """)
    db.commit()
    t1 = time.time()
    logger.info('time taken for upsert to geo_location: {}'.format(round(t1-t0)))

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
    sirene_stocketab.geo_md5 = t2.geo_md5

        """
    )
    db.commit()
    cursor.execute("""truncate table sirene_stocketab_staging""")
    db.commit()
    t1 = time.time()
    logger.info('time taken for upsert to live etab table: {}'.format(round(t1-t0)))

files_url = 'https://files.data.gouv.fr/insee-sirene/'

# place file names here
# filestring=['2024-06-01-StockEtablissement_utf8.zip', '2024-05-06-StockUniteLegale_utf8.zip']

# for testing purposes, we are running it on one file
filestring = '2024-06-01-StockUniteLegale_utf8.zip'
def process_download(filestring: str):
    """check for file, if not exists, download, unzip and split"""
    # build the url for the request, by appending filestring var to files_url
    list_of_zip_files = os.listdir()
    downloaded = 0
    for file in list_of_zip_files:
        if '.zip' in file and file == filestring:
            logger.info('{} has been found'.format(filestring))
            downloaded = 1

    if downloaded == 0:
        request_url = files_url + filestring

        # send a request to recieve the file
        r = requests.get(request_url, stream=True, verify=False)

        # if we recieve a 200, that files exists and we can continue
        if r.status_code != 200:
            logger.error('status code')
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

    # unzip the file and delete the zip file
    with zipfile.ZipFile(filestring, 'r') as zip_ref:
        zip_ref.extractall()
        infolist = zip_ref.infolist()
        if infolist:
            output = infolist[0].filename
        zip_ref.close()

    logger.info('file extracted successfully')
    logger.info('outputfile = {}'.format(output))

    # remove zip file here
    os.remove(filestring)


    # we use filesplit.split Split to divide the file into
    # smaller batches of 50,000 lines
    split = Split(output, 'fragments')
    split.bylinecount(linecount=50000, includeheader=True)
    os.remove('fragments/manifest')

    # once this is done, we can delete the unzipped csv
    # os.remove(output)

if __name__ == '__main__':
    # download given filename
    process_download(filestring=filestring)
    # once this is done, we loop over each fragment and load it into the database
    list_of_fragments = os.listdir('fragments')
    for fragment in list_of_fragments:
        if 'Etablissement' in filestring and 'Etablissement' in fragment:
            process_etab_fragment(filename='fragments/' + fragment)
            os.remove('fragments/' + fragment)
        elif 'Legal' in filestring and 'Legal' in fragment:
            process_legal_fragment(filename='fragments/' + fragment)
            os.remove('fragments/' + fragment)

