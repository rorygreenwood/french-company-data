from download_files import process_download, split_file, unzip_file
from larger_legal_file_process import legal_file_process
import time
import datetime
import os

import polars as pl
import logging

from utils import connect_preprod, pipeline_messenger, constring

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
        '00': 'Collective Investment', # Organisme de placement collectif en valeurs mobilières sans personnalité morale
        '10': 'Entrepreneur', # Entrepreneur individuel

        '21': 'Joint Ownership', # Indivision
        '22': 'De facto Corporation', # Société créée de fait
        '23': 'Joint-stock Company', # Societe en participaiton
        '24': 'Trust', # Fiducie
        '27': 'Parish', # Paroisse
        '28': 'Subject to VAT', # Assujettie unique a la TVA
        '29': 'Private Law Group without legal personality', # Autre groupement de droit privé non doté de la personnalité morale

        '31': 'Legal Entity Under Foreign Law, RCS registered', # Personne morale de droit étranger, immatriculée au RCS (registre du commerce et des sociétés)
        '32': 'Legal Entity Under Foreign Law, not RCS registered', # Personne morale de droit étranger, non immatriculée au RCS

        '41': 'Public Company of industrial/commercial nature', # Etablissement public ou régie à caractère industriel ou commercial

        '51': 'Limited Liability Co-operative', # Société coopérative commerciale particulière
        '52': 'SNC (General Partnership)', # Société en nom collectif (SNC)
        '53': 'SCA (Limited Partnership)', # Société en commandite (SCA)
        '54': 'SARL (Limited Liability Company)', # Société à responsabilité limitée (SARL)
        '55': 'SA (Limited Company with Board of Directors)', # Société anonyme à conseil d'administration (SA)
        '56': 'SA (Limited Company with Management Board)', # Société anonyme à directoire (SA)
        '57': 'SAS (Joint-Stock Company)', # Société par actions simplifiée (SAS)
        '58': 'SE (EU Registered Company)', # Société européenne (SE)

        '61': 'Pension Funds', # Caisse d'épargne et de prévoyance
        '62': 'Economic Interest Group', # Groupement d'intérêt économique
        '63': 'Agricultural Co-operative', # Société coopérative agricole
        '64': 'Mutual Insurance', # Société d'assurance mutuelle
        '65': 'SC (Civil Company)', # Société civile
        '69': 'Other Registered Private Company', # Autre personne morale de droit privé inscrite au registre du commerce et des sociétés
        '71': 'State Administration', # Administration de l'état
        '72': 'Territorial Authority', # Collectivité territoriale
        '73': 'Public Administration', # Etablissement public administratif
        '74': 'Other Public Entity', # Autre personne morale de droit public administratif

        '81': 'Social Security', # Organisme gérant un régime de protection sociale à adhésion obligatoire

        '82': 'Mutual Organisation', # Organisme mutualiste
        '83': 'Council', # Comité d'entreprise
        '84': 'Professional Organisation', # Organisme professionnel
        '85': 'Non-compulsory pension', # Organisme de retraite à adhésion non obligatoire

        '91': 'Union', # Syndicat de propriétaires
        '92': '1901 Association', # Association loi 1901 ou assimilé
        '93': 'Foundation', # Fondation
        '99': 'Other Legal Entity'
    }

    return company_type_map[input_dict['LegalCategory'][0:2]]

def process_legal_fragment(filename: str) -> None:
    """
    main process to write to StockLegale
    upserts to organisation and naf_code
    :param filename:
    :return:
    """

    pldf = pl.read_csv(filename, dtypes={
                                         'company_number': pl.Utf8,
                                         'siret': pl.Utf8,
                                         'LegalCategory': pl.Utf8,
                                         'EmployeeCountCategory': pl.Utf8})
    # sending polars dataframe to staging table
    t0 = time.time()
    pldf.write_database(table_name='sirene_stocklegal_staging',
                        connection_uri=constring, if_exists='replace',
                        )
    t1 = time.time()

    logger.info('time taken to write stock legal into staging: {}'.format(round(t1 - t0)))
    # upsert into organisation
    cursor.execute(
        """
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
    organisation.last_modified_date = sirene_stocklegal_staging.last_modified_date"""
    )
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
    sirene_stocklegal.country = t2.country,
    sirene_stocklegal.country_code = t2.country_code,
    sirene_stocklegal.last_modified_by = t2.last_modified_by,
    sirene_stocklegal.last_modified_date = t2.last_modified_date
        """
    )
    db.commit()
    cursor.execute("""truncate table sirene_stocklegal_staging""")
    db.commit()
    t1 = time.time()
    logger.info('time taken to upsert into live tables: {}'.format(round(t1-t0)))

def run_legal():
    # in the future, this will be the curdate month
    current_date_month = datetime.datetime.now().month
    current_date_year = datetime.datetime.now().year
    filestring = f'{current_date_year}-{current_date_month:02d}-01-StockUniteLegale_utf8.zip'
    logger.info(f'sending request with filestring: {filestring}')

    # check if zipfile is not already in the dir
    if filestring not in os.listdir():
        t0 = time.time()
        # download file
        # zipped_file = '2024-08-01-StockUniteLegale_utf8.zip'
        zipped_file = process_download(filestring=filestring)
        # unzip file
        unzipped_file = unzip_file(filestring=zipped_file)
        # process unzipped file
        processed_file = legal_file_process(filename=unzipped_file)
        # split processed file
        split_file(processed_file)
        t1 = time.time()
        download_time = round(t1 - t0)
        logger.info(f'download and processing time: {download_time}')
    else:
        logger.info('file already downloaded')

    # process_download leaves the section fragments to be processed
    list_of_fragments = os.listdir('fragments')
    frag_count = 1
    try:
        t0 = time.time()
        fragment_times = []
        logger.debug('processing fragments')
        for fragment in list_of_fragments:
            logger.info(fragment)
            if 'Legal' in filestring and 'Legal' in fragment:
                f_t0 = time.time()
                process_legal_fragment(filename='fragments/' + fragment)
                os.remove('fragments/' + fragment)
                frag_count += 1
                f_t1 = time.time()
                fragment_time_taken = round(f_t1 - f_t0)
                fragment_times.append(fragment_time_taken)
        t1 = time.time()
        time_taken = t1 - t0
        logger.info('total time for processing: {}'.format(time_taken))
        avg_time_taken = round(sum(fragment_times) / (len(fragment_times)-1), 2)
        logger.info('average fragment processing time: {}'.format(avg_time_taken))


        pipeline_messenger(
            title='Sirene Stock Unite Legale Pipeline has run',
            text=f'time taken: {time_taken}\n average time per fragment: {avg_time_taken}',
            notification_type='pass'
        )
    except Exception as e:
        pipeline_messenger(
            title='Sirene Stock Unite Legale Pipeline has failed',
            text= f'Error in file: {filestring} - {e}',
            notification_type='fail'
        )

if __name__ == '__main__':
    run_legal()
