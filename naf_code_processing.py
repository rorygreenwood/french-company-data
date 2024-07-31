"""
a separate process to the legal_processing

1. download the unitelegale file from sirene (or from aws)
2. unzip the file
3. load the file into pandas dataframe
4. remove records for inactive companies, and ensure all naf codes are in a single column using unions
5. group records by their naf code
6. insert grouping counts into staging table
7. write data into naf code

why don't we use the naf_code table?
it has records from companies that no longer exist, and these would skew the results.
"""
import logging
import datetime
import re
import polars as pl
from utils import constring, connect_preprod
from hashlib import md5

cursor, db = connect_preprod()

format_str = "[%(levelname)s: %(lineno)d] %(message)s"
logging.basicConfig(level=logging.INFO, format=format_str)
logger = logging.getLogger(__name__)

filename = '2024-07-01-StockUniteLegale_utf8.zip'
date_of_file = re.findall(string=filename, pattern='[0-9]{4}-[0-9]{2}-[0-9]{2}')[0]
date_object = datetime.datetime.strptime(date_of_file, "%Y-%m-%d")
month_object = f'{date_object.year}-{date_object.month:02d}-01'

test_file = 'StockUniteLegale_utf8.csv'
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
    'anneeEffectifsUniteLegale': 'EmployeeCountCategoryDateUpdated',  # year when the employee number was last recorded
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

df = pl.read_csv(test_file, dtypes={'codeCommuneEtablissement': pl.Utf8,
                                         'siren': pl.Utf8,
                                         'siret': pl.Utf8,
                                         'categorieJuridiqueUniteLegale': pl.Utf8,
                                         'trancheEffectifsUniteLegale': pl.Utf8})

naf_code_df = pl.read_csv('_naf_code_data.csv', dtypes={'iSIC_code': pl.Utf8})

# rename columns to english translations
df = df.rename(unite_legale_cols)
logger.info(df.columns)



# filter for active entitites
df = df.filter(pl.col('AdministrativeStatus') == 'A')

# filter for companies only (0000 means it is a person)
df = df.filter(pl.col('LegalCategory') != '0000')

# filter for the current naf code revision
df = df.filter(pl.col('ActiveLegalUnit') == 'NAFRev2')

# get a small subset of the data
df = df.select(['NAFCategory'])

# groupby function to get counts for the data based on naf codes
naf_counts = df['NAFCategory'].value_counts()

# pair data with naf code descriptions and isic data
naf_counts_join = naf_counts.join(other=naf_code_df, left_on='NAFCategory', right_on='naf_code', how='left')

# add the filedate
naf_counts_join = naf_counts_join.with_columns(file_date=pl.lit(month_object))

print(naf_counts_join.columns)
# add an md5 of file date and isic value
def gen_isic_category_md5(input_dict: dict) -> str:
    """
    generates a md5 str based on the category and file date variable
    :param input_dict:
    :return:
    """
    if input_dict['iSIC_code'] is not None and input_dict['file_date'] is not None:
        concatenated_str = input_dict['iSIC_code'] + str(input_dict['file_date'])
        hash_object = md5(concatenated_str.encode())
        return hash_object.hexdigest()
    else:
        return 'fail'
naf_counts_join = naf_counts_join.with_columns(pl.struct(['iSIC_code', 'file_date']).apply(gen_isic_category_md5).alias('md5_str'))
# send to staging table
naf_counts_join.write_database('naf_code_counts_staging', connection_uri=constring, if_exists='replace')

cursor.execute("""insert into isic_code_counts (month, isic_code, naf_code, naf_count, md5_str, last_modified_date, last_modified_by)
select file_date, iSIC_code, NAFCategory, counts, md5_str, now(), 'naf_code_insert' from naf_code_counts_staging
on duplicate key update 
naf_code = NAFCategory,
naf_count = counts,
last_modified_date = NOW(),
last_modified_by = 'naf_code_update'
""")
db.commit()
