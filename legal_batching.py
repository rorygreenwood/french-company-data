import traceback
from fileinput import filename

from download_files import process_download
import time
import zipfile
import datetime
import os

import polars as pl
import logging

from utils import connect_preprod, pipeline_messenger, constring, file_cleanup, zip_csv_to_parquet

cursor, db = connect_preprod()
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(filename)s line:%(lineno)d %(message)s')


def map_employee_count(input_dict: dict) -> str:
    """
    sirene provides a code for each number of employees
    these are mapped to a table in the documentation
     made available at https://www.sirene.fr/static-resources/htm/v_sommaire_311.htm#27
    :param input_dict:
    :return:
    """
    tranche_effectis_dict = {  # dictionary of what each number means in terms of workers
        '0': '0 fulltime employees',
        '00': '0 fulltime employees',
        '1': '1-2 employees',
        '01': '1-2 employees',
        '2': '2-3 employees',
        '02': '2-3 employees',
        '3': '6-9 employees',
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
        'NN': 'no number submitted'
    }
    if input_dict['EmployeeCountCategory'] is not None:
        return tranche_effectis_dict[str(input_dict['EmployeeCountCategory'])]
    else:
        return 'NA'


def create_org_id(input_dict: dict) -> str:
    """
    create an organisation id for matching in organisation
    :param input_dict:
    :return:
    """
    return 'FR' + str(input_dict['company_number'])


def map_company_activity(input_dict: dict) -> str:
    """
    takes etatAdministratifUniteLegale and turns it into Active/Inactive
    :param input_dict:
    :return:
    """
    if input_dict['AdministrativeStatus'] == 'A':
        return 'Active'
    elif input_dict['AdministrativeStatus'] == 'C':
        return 'Inactive'


def map_company_type(input_dict: dict) -> str:
    """
    index the first two numbers of the code, and map it to the first two digits of the mapping csv provided by sirene
    and found at https://www.sirene.fr/static-resources/htm/v_sommaire_311.htm#27
    :param input_dict:
    :return:
    """

    company_type_map = {
        '00': 'Collective Investment',
        # Organisme de placement collectif en valeurs mobilières sans personnalité morale
        '10': 'Entrepreneur',  # Entrepreneur individuel

        '21': 'Joint Ownership',  # Indivision
        '22': 'De facto Corporation',  # Société créée de fait
        '23': 'Joint-stock Company',  # Societe en participaiton
        '24': 'Trust',  # Fiducie
        '27': 'Parish',  # Paroisse
        '28': 'Subject to VAT',  # Assujettie unique a la TVA
        '29': 'Private Law Group without legal personality',
        # Autre groupement de droit privé non doté de la personnalité morale

        '31': 'Legal Entity Under Foreign Law, RCS registered',
        # Personne morale de droit étranger, immatriculée au RCS (registre du commerce et des sociétés)
        '32': 'Legal Entity Under Foreign Law, not RCS registered',
        # Personne morale de droit étranger, non immatriculée au RCS

        '41': 'Public Company of industrial/commercial nature',
        # Etablissement public ou régie à caractère industriel ou commercial

        '51': 'Limited Liability Co-operative',  # Société coopérative commerciale particulière
        '52': 'SNC (General Partnership)',  # Société en nom collectif (SNC)
        '53': 'SCA (Limited Partnership)',  # Société en commandite (SCA)
        '54': 'SARL (Limited Liability Company)',  # Société à responsabilité limitée (SARL)
        '55': 'SA (Limited Company with Board of Directors)',  # Société anonyme à conseil d'administration (SA)
        '56': 'SA (Limited Company with Management Board)',  # Société anonyme à directoire (SA)
        '57': 'SAS (Joint-Stock Company)',  # Société par actions simplifiée (SAS)
        '58': 'SE (EU Registered Company)',  # Société européenne (SE)

        '61': 'Pension Funds',  # Caisse d'épargne et de prévoyance
        '62': 'Economic Interest Group',  # Groupement d'intérêt économique
        '63': 'Agricultural Co-operative',  # Société coopérative agricole
        '64': 'Mutual Insurance',  # Société d'assurance mutuelle
        '65': 'SC (Civil Company)',  # Société civile
        '69': 'Other Registered Private Company',
        # Autre personne morale de droit privé inscrite au registre du commerce et des sociétés
        '71': 'State Administration',  # Administration de l'état
        '72': 'Territorial Authority',  # Collectivité territoriale
        '73': 'Public Administration',  # Etablissement public administratif
        '74': 'Other Public Entity',  # Autre personne morale de droit public administratif
        '75': 'Unknown - TBD',
        '81': 'Social Security',  # Organisme gérant un régime de protection sociale à adhésion obligatoire

        '82': 'Mutual Organisation',  # Organisme mutualiste
        '83': 'Council',  # Comité d'entreprise
        '84': 'Professional Organisation',  # Organisme professionnel
        '85': 'Non-compulsory pension',  # Organisme de retraite à adhésion non obligatoire

        '91': 'Union',  # Syndicat de propriétaires
        '92': '1901 Association',  # Association loi 1901 ou assimilé
        '93': 'Foundation',  # Fondation
        '99': 'Other Legal Entity'
    }

    return company_type_map[input_dict['LegalCategory'][0:2]]


def read_file_in_chunks(file_path, chunk_size: int):
    df = pl.scan_parquet(file_path)
    offset = 0
    while True:
        batch = df.slice(offset=offset, length=chunk_size).collect()
        if batch.shape[0] == 0:
            break
        yield batch
        offset += chunk_size


def process_batch(df: pl.DataFrame,
                  zipfile: str,
                  is_live: bool = True) -> None:
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
        'categorieEntreprise': 'BusinessCategory',  # either SME (small-medium enterprise), Medium (ETI) or GE (Large)
        'anneeCategorieEntreprise': 'YearOfBusinessCategoryAssignment',  #
        'dateDebut': 'DateOfBusinessStart',  #
        'etatAdministratifUniteLegale': 'AdministrativeStatus',  # A means active, C means inactive
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

    original_df_size = len(df)

    df = df.rename(unite_legale_cols)

    # remove records where a company name is not found
    # perform a wider filtering of [ND], if a record has a lot of [ND] fields, especially fields we need, omit the record
    df = df.filter(pl.col('LegalEntityName') != '[ND]')
    df.drop_nulls(subset='LegalEntityName')

    # drop records where the legal category is 0000; these are people and not companies
    df = df.filter(pl.col('LegalCategory') != '0000')

    # for now, we are only accepting LegalCategory 5xxx, as these are societe commerciale and the priority
    logger.debug(f'size of file before filtering category for dataframe: {len(df)}')

    # filtering only on societe commercial
    # df = df.filter(pl.col('LegalCategory').str.slice(0, 1) == '5')
    logger.debug(f'size of file after filtering category for dataframe: {len(df)}')

    # map company_type ids
    df = df.with_columns(
        pl.struct(['LegalCategory']).map_elements(map_company_type, return_dtype=pl.Utf8).alias('company_type'))

    # writeup company id
    df = df.with_columns(pl.struct(['company_number']).map_elements(create_org_id, return_dtype=pl.Utf8).alias('id'))

    # add additional columns required from organisation insert
    df = df.with_columns(country=pl.lit('FRANCE'),
                         country_code=pl.lit('FR'))

    # determine whether or not the company is active or inactive
    df = df.with_columns(
        pl.struct(['AdministrativeStatus']).map_elements(map_company_activity, return_dtype=pl.Utf8).alias(
            'company_status'))

    # map the category provided by siren to their documentation to get a range of numbers for employees, rather than a
    # representative category
    df = df.with_columns(
        pl.struct(['EmployeeCountCategory']).map_elements(map_employee_count, return_dtype=pl.Utf8).alias(
            'EmployeeCount'))
    t1 = time.time()

    # for diagnostic purposes, add filenames and update times into the dataframe
    df = df.with_columns(pl.lit(zipfile).alias('last_modified_by'))
    df = df.with_columns(pl.lit(datetime.datetime.now()).alias('last_modified_date'))

    new_df_size = len(df)


    logger.info(f'size of original df: {original_df_size}')
    logger.info(f'size of df: {new_df_size}')
    logger.info(f'pct change in filesize: {round((original_df_size - new_df_size) / original_df_size * 100, 2)}')

    logger.info('time taken to prepare stock legal: {}s'.format(round(t1 - t0)))

    # sending polars dataframe to staging table
    cursor.execute("""truncate table sirene_stocklegal_staging""")
    db.commit()

    logger.info('writing to db')
    t0 = time.time()
    df.write_database(table_name='sirene_stocklegal_staging',
                      connection=constring, if_table_exists='append',
                      )
    t1 = time.time()
    logger.info('time taken to write stock legal into staging: {}'.format(round(t1 - t0)))

    if is_live:
        # upsert into organisation
        cursor.execute("""
        insert into organisation (
        id,
        company_number,
        company_name,
        company_status,
        country,
        date_formed,
        company_type,
        last_modified_by,
        last_modified_date,
        country_code)
    
        select
        id,
        company_number,
        LegalEntityName,
        company_status,
        country,
        DateCreated,
        company_type,
        last_modified_by,
        last_modified_date,
        'FR' as country_code
        from sirene_stocklegal_staging
    
        on duplicate key update
        company_name = LegalEntityName, 
        organisation.company_status = sirene_stocklegal_staging.company_status,
        organisation.company_type = sirene_stocklegal_staging.company_type,
        organisation.last_modified_by = sirene_stocklegal_staging.last_modified_by,
        organisation.last_modified_date = sirene_stocklegal_staging.last_modified_date""")
        db.commit()
        t0 = time.time()

        # insert naf code data into NAF code
        cursor.execute(
            """
            insert into naf_code (code, organisation_id, name_en, name_fr, last_modified_date, last_modified_by) 
    
            select  NAFCategory, id, t2.name_en, t2.name_fr, last_modified_date, last_modified_by
            from sirene_stocklegal_staging t1
    
            inner join naf_codes_translations t2
            on t1.NAFCategory = t2.code
            where 
            t1.AdministrativeStatus = 'A'
            and t1.ActiveLegalUnit = 'NAFRev2'
            on duplicate key update last_modified_date = curdate(), last_modified_by = 'stock legal pipeline update'
            """
        )
        db.commit()

        t1 = time.time()
        logger.info('time taken to insert NAF codes into staging: {}'.format(round(t1 - t0)))

        # upsert staging table into main stock_legal table
        t0 = time.time()
        cursor.execute(
            """
            insert into sirene_stocklegal
                (company_number, 
                LegalUnitBroadcastID, 
                PurgeStatus, 
                DateCreated, 
                LegalAcronym, 
                GenderOfPerson, 
                NaturalName1, 
                NaturalName2, 
                NaturalName3, 
                NaturalName4, 
                PreferredName, 
                pseudonym, 
                RNANumber, 
                EmployeeCountCategory, 
                EmployeeCountCategoryDateUpdated, 
                LegalUnitUpdated, 
                TimeAsLegalUnit, 
                BusinessCategory, 
                YearOfBusinessCategoryAssignment, 
                DateOfBusinessStart, 
                AdministrativeStatus, 
                PersonBirthName, 
                PersonUsedName, 
                LegalEntityName, 
                LegalEntityName1, 
                LegalEntityName2, 
                LegalEntityName3, 
                LegalCategory, 
                NAFCategory, 
                ActiveLegalUnit, 
                NICAssignment, 
                SSEBool, 
                MissionDrivenCompanyBool, 
                EmployerNature, 
                id, 
                country, 
                country_code, 
                last_modified_by, 
                EmployeeCount, 
                company_type, 
                company_status, 
                last_modified_date) 
            select 
                company_number, 
                LegalUnitBroadcastID, 
                PurgeStatus, 
                DateCreated, 
                LegalAcronym, 
                GenderOfPerson, 
                NaturalName1, 
                NaturalName2, 
                NaturalName3, 
                NaturalName4, 
                PreferredName, 
                pseudonym, 
                RNANumber, 
                EmployeeCountCategory, 
                EmployeeCountCategoryDateUpdated, 
                LegalUnitUpdated, 
                TimeAsLegalUnit, 
                BusinessCategory, 
                YearOfBusinessCategoryAssignment, 
                DateOfBusinessStart, 
                AdministrativeStatus, 
                PersonBirthName, 
                PersonUsedName, 
                LegalEntityName, 
                LegalEntityName1, 
                LegalEntityName2, 
                LegalEntityName3, 
                LegalCategory, 
                NAFCategory, 
                ActiveLegalUnit, 
                NICAssignment, 
                SSEBool, 
                MissionDrivenCompanyBool, 
                EmployerNature, 
                id, 
                country, 
                country_code, 
                last_modified_by, 
                EmployeeCount, 
                company_type, 
                company_status, 
                last_modified_date 
            from sirene_stocklegal_staging t2
            on duplicate key update 
                company_number = t2.company_number,
                LegalUnitBroadcastID = t2.LegalUnitBroadcastID,
                PurgeStatus = t2.PurgeStatus,
                DateCreated = t2.DateCreated,
                LegalAcronym = t2.LegalAcronym,
                GenderOfPerson = t2.GenderOfPerson,
                NaturalName1 = t2.NaturalName1,
                NaturalName2 = t2.NaturalName2,
                NaturalName3 = t2.NaturalName3,
                NaturalName4 = t2.NaturalName4,
                PreferredName = t2.PreferredName,
                pseudonym = t2.pseudonym,
                RNANumber = t2.RNANumber,
                EmployeeCountCategory = t2.EmployeeCountCategory,
                EmployeeCountCategoryDateUpdated = t2.EmployeeCountCategoryDateUpdated,
                LegalUnitUpdated = t2.LegalUnitUpdated,
                TimeAsLegalUnit = t2.TimeAsLegalUnit,
                BusinessCategory = t2.BusinessCategory,
                YearOfBusinessCategoryAssignment = t2.YearOfBusinessCategoryAssignment,
                DateOfBusinessStart = t2.DateOfBusinessStart,
                AdministrativeStatus = t2.AdministrativeStatus,
                PersonBirthName = t2.PersonBirthName,
                PersonUsedName = t2.PersonUsedName,
                LegalEntityName = t2.LegalEntityName,
                LegalEntityName1 = t2.LegalEntityName1,
                LegalEntityName2 = t2.LegalEntityName2,
                LegalEntityName3 = t2.LegalEntityName3,
                LegalCategory = t2.LegalCategory,
                NAFCategory = t2.NAFCategory,
                ActiveLegalUnit = t2.ActiveLegalUnit,
                NICAssignment = t2.NICAssignment,
                SSEBool = t2.SSEBool,
                MissionDrivenCompanyBool = t2.MissionDrivenCompanyBool,
                EmployerNature = t2.EmployerNature,
                country = t2.country,
                country_code = t2.country_code,
                last_modified_by = t2.last_modified_by,
                last_modified_date = t2.last_modified_date""")
        db.commit()

        t1 = time.time()
        logger.info('time taken to upsert into live tables: {}'.format(round(t1 - t0)))


def verify_pipeline(file_length: int, file_name: str) -> bool:
    cursor.execute("""select * from sirene_stocklegal where last_modified_by = %s""", (file_name,))
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


legal_file_schema = {'codeCommuneEtablissement': pl.Utf8,
                     'siren': pl.Utf8,
                     'siret': pl.Utf8,
                     'categorieJuridiqueUniteLegale': pl.Utf8,
                     'trancheEffectifsUniteLegale': pl.Utf8}


def main():
    # in the future, this will be the curdate month
    current_date_month = datetime.datetime.now().month
    current_date_year = datetime.datetime.now().year
    filestring = f'{current_date_year}-{current_date_month:02d}-01-StockUniteLegale_utf8.zip'
    csv_file = 'StockUniteLegale_utf8.csv'
    parquet_file = 'StockUniteLegale_utf8.parquet'
    logger.info(f'sending request with filestring: {filestring}')

    t0 = time.time()

    # download file

    zipped_file = process_download(filestring=filestring)
    file_size = zip_csv_to_parquet(zip_file_path=zipped_file,
                                   parquet_name=parquet_file,
                                   schema=legal_file_schema
                                   )

    chunk_size = 150000

    for df in read_file_in_chunks(parquet_file, chunk_size=chunk_size):
        try:
            process_batch(df=df, zipfile=zipped_file)
        except Exception as e:
            logger.error(e)
            df.write_csv('error.csv')
            pipeline_messenger(
                title='French Legal Error',
                text=f'{e}: {traceback.format_exc()}',
                notification_type='fail'
            )
            file_list = [filestring, csv_file]
            file_cleanup(file_list=file_list)
            quit()
    t1 = time.time()
    if verify_pipeline(file_length=file_size, file_name=zipped_file):
        pipeline_messenger(
            title='French Legal Pass',
            text=f'time taken: {round(t1 - t0, 2)} seconds',
            notification_type='success'
        )

        file_list = [filestring, csv_file]
        file_cleanup(file_list=file_list)


if __name__ == '__main__':
    main()
