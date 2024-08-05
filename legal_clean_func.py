"""
To reduce the number of files being processed, we process the file as a whole before splitting it
"""
import os

import polars as pl
import time
import logging
import datetime

format_str = "[%(levelname)s: %(lineno)d] %(message)s"
logging.basicConfig(level=logging.INFO, format=format_str)
logger = logging.getLogger(__name__)

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


def legal_file_process(filename) -> str:
    """
    This function is used to process the UniteLegale .csv file as a whole before splitting it
    :param filename:
    :return:
    """
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
        'anneeEffectifsUniteLegale': 'EmployeeCountCategoryDateUpdated', # year when the employee number was last recorded
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
    pldf = pl.read_csv(filename, dtypes={'codeCommuneEtablissement': pl.Utf8,
                                         'siren': pl.Utf8,
                                         'siret': pl.Utf8,
                                         'categorieJuridiqueUniteLegale': pl.Utf8,
                                         'trancheEffectifsUniteLegale': pl.Utf8},

                       ignore_errors=False)

    original_pldf_size = len(pldf)

    pldf = pldf.rename(unite_legale_cols)

    # remove records where a company name is not found
    # perform a wider filtering of [ND], if a record has a lot of [ND] fields, especially fields we need, omit the record
    pldf = pldf.filter(pl.col('LegalEntityName') != '[ND]')
    pldf.drop_nulls(subset='LegalEntityName')

    # drop records where the legal category is 0000; these are people and not companies
    pldf = pldf.filter(pl.col('LegalCategory') != '0000')

    # for now, we are only accepting LegalCategory 5xxx, as these are societe commerciale and the priority
    logger.debug(f'size of file before filtering category for {filename}: {len(pldf)}')

    # filtering only on societe commercial
    pldf = pldf.filter(pl.col('LegalCategory').str.slice(0,1) ==  '5')
    logger.debug(f'size of file after filtering category for {filename}: {len(pldf)}')

    # map company_type ids
    pldf = pldf.with_columns(pl.struct(['LegalCategory']).apply(map_company_type, return_dtype=pl.Utf8).alias('company_type'))

    # writeup company id
    pldf = pldf.with_columns(pl.struct(['company_number']).apply(create_org_id, return_dtype=pl.Utf8).alias('id'))

    # add additional columns required from organisation insert
    pldf = pldf.with_columns(country=pl.lit('FRANCE'),
                      country_code=pl.lit('FR'))

    # determine whether or not the company is active or inactive
    pldf = pldf.with_columns(pl.struct(['AdministrativeStatus']).apply(map_company_activity, return_dtype=pl.Utf8).alias('company_status'))

    # map the category provided by siren to their documentation to get a range of numbers for employees, rather than a
    # representative category
    pldf = pldf.with_columns(pl.struct(['EmployeeCountCategory']).apply(map_employee_count, return_dtype=pl.Utf8).alias('EmployeeCount'))
    t1 = time.time()

    # for diagnostic purposes, add filenames and update times into the dataframe
    pldf = pldf.with_columns(pl.lit(filename + ' - insert').alias('last_modified_by'))
    pldf = pldf.with_columns(pl.lit(datetime.datetime.now()).alias('last_modified_date'))

    new_pldf_size = len(pldf)

    logger.info(f'size of file: {new_pldf_size}')
    logger.info(f'size of original file: {original_pldf_size}')
    logger.info(f'change in filesize: {round((original_pldf_size - new_pldf_size) / original_pldf_size * 100, 2)}')

    logger.info('time taken to prepare stock legal: {}s'.format(round(t1 - t0)))

    # export to csv that will be fragmented
    pldf.write_csv('StockUniteLegale_clean.csv')

    # remove original file
    os.remove(filename)

    return 'StockUniteLegale_clean.csv'



