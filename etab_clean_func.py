import polars as pl
import time
import logging
import datetime
import hashlib

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


def etab_file_process(filename: str) -> str:
    """
    Process StockEtablissement
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

    t0 = time.time()
    pldf = pl.read_csv(filename, dtypes={'codeCommuneEtablissement': pl.Utf8,
                                         'codeCedexEtablissement': pl.Utf8,
                                         'numeroVoieEtablissement': pl.Utf8,
                                         'codePostalEtablissement': pl.Utf8,
                                         'numeroVoie2Etablissement': pl.Utf8,
                                         'codePostal2Etablissement': pl.Utf8,
                                         'distributionSpecialeEtablissement': pl.Utf8,
                                         'complementAdresseEtablissement': pl.Utf8,
                                         'siren': pl.Utf8}, ignore_errors=True,
                       null_values=['[ND]', 'NN'])
    pldf = pldf.rename(unite_etab_cols)
    pldf = pldf.fill_null('')
    pldf = pldf.fill_nan('')
    pldf = pldf.with_columns(pl.struct(['company_number']).apply(create_org_id, return_dtype=pl.Utf8).alias('id'))

    # get original size for analytics
    original_pldf_size = len(pldf)

    # todo remove closed addresses
    logger.debug(f'size of file before removing closed offices for {filename}: {len(pldf)}')
    pldf = pldf.filter(pl.col('AdministrativeStatus') != 'F')
    logger.debug(f'size of file after removing closed offices for {filename}: {len(pldf)}')

    # generate md5 hash
    pldf = pldf.with_columns(
        pl.struct(['id', 'AddressPostcode']).apply(generate_geo_md5, return_dtype=pl.Utf8).alias('geo_md5'))

    # create first line of address
    # todo exceptions.ComputeError: TypeError: sequence item 0: expected str instance, NoneType found

    pldf = pldf.with_columns(
        pl.struct(['AddressBuildingBlock', 'AddressNumber', 'AddressNumberSubUnit']).apply(create_address_line_1).alias(
            'address_line_1'))

    # create second line of address
    pldf = pldf.with_columns(pl.struct(
        ['AddressUniqueIdentifier', 'AddressLabel']).apply(
        create_address_line_2).alias('address_line_2'))

    # determine whether the office is a head office or no
    pldf = pldf.with_columns(
        pl.struct(['RegisteredOfficeBool']).apply(assign_office_type).alias('registered_office_type'))

    t1 = time.time()

    # for diagnostic purposes, add filenames and update times into the dataframe
    pldf = pldf.with_columns(pl.lit(filename + ' - insert').alias('last_modified_by'))
    pldf = pldf.with_columns(pl.lit(datetime.datetime.now()).alias('last_modified_date'))

    new_pldf_size = len(pldf)

    logger.info(f'size of file: {new_pldf_size}')
    logger.info(f'size of original file: {original_pldf_size}')
    logger.info(f'change in filesize: {round((original_pldf_size - new_pldf_size) / original_pldf_size * 100, 2)}')

    logger.info('Preparing etab file in {} seconds'.format(round(t1 - t0)))

    pldf.write_csv('StockEtablissement_clean.csv')
    return 'StockEtablissement_clean.csv'