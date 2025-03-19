import unittest
import os
import unittest
from utils import zip_csv_to_parquet, pipeline_messenger, connect_preprod, logger
from etab_batching import process_batch as etab_process_batch, etab_schema, read_file_in_chunks

cursor, db = connect_preprod()


class TestReadBatchEtab(unittest.TestCase):
    def test(self):
        logger.info(f'beginning {TestReadBatchEtab.__name__}')
        test_len = 49999
        i = 0
        for file in read_file_in_chunks(file_path='test_etab.parquet', chunk_size=10000):
            i += len(file)

        assert i == test_len
        logger.info(f'end {TestReadBatchEtab.__name__}')


class TestReadParquetEtabStaging(unittest.TestCase):
    def test(self):
        logger.info(f'beginning {TestReadParquetEtabStaging.__name__}')
        for file in read_file_in_chunks(file_path='test_etab.parquet', chunk_size=10000):
            etab_process_batch(df=file, zipfile='test', is_live=False)

            # find out length of fragment
            cursor.execute('select count(*) from sirene_stocketab_staging')
            res = cursor.fetchone()[0]
            logger.info(res)
            assert res > 0

            cursor.execute("""truncate table sirene_stocketab_staging""")
            db.commit()

            # only need to do once
            break
        logger.info(f'end {TestReadParquetEtabStaging.__name__}')


class TestReadParquetEtabStagingToMain(unittest.TestCase):
    test_etab_table = 'sirene_test_stocketab'
    test_geo_location = 'sirene_test_geolocation'

    def setUp(self):
        logger.info(f'beginning {TestReadParquetEtabStagingToMain.__name__}')
        # create a test table and use this for insert and upsert

        # try not to affect actual tables
        assert 'test' in self.test_etab_table
        assert 'test' in self.test_geo_location

        cursor.execute(f'create table if not exists {self.test_etab_table} like sirene_stocketab')
        db.commit()

        cursor.execute(f'create table if not exists {self.test_geo_location} like geo_location')
        db.commit()

    def test(self):
        logger.info('test start...')
        for file in read_file_in_chunks(file_path='test_etab.parquet', chunk_size=10000):
            etab_process_batch(df=file, zipfile='test', is_live=False)

            # get count in staging
            cursor.execute('select count(*) from sirene_stocketab_staging')
            staging_res = cursor.fetchone()[0]

            # add upserts to test geolocation here
            cursor.execute(f"""
            insert into {self.test_geo_location} (
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
            last_modified_by = %s""", ('test_insert', 'test_update'))
            db.commit()

            # get size of geolocation
            cursor.execute(f'select count(*) from {self.test_geo_location}')
            geo_location_res = cursor.fetchone()[0]
            logger.info(f'size of geolocation res: {geo_location_res}')
            logger.info(f'size of staging res: {staging_res}')
            assert geo_location_res < staging_res

            cursor.execute(f"""
            insert into {self.test_etab_table}
            select * from sirene_stocketab_staging t2
            on duplicate key update
            company_number = t2.company_number,
            localnic = t2.localnic,
            siret = t2.siret,
            distributionStatus = t2.distributionStatus,
            EstablishmentDate = t2.EstablishmentDate,
            EmployeeCountCategory = t2.EmployeeCountCategory,
            EmployeeCountCategoryYear = t2.EmployeeCountCategoryYear,
            mainNAF = t2.mainNAF,
            LastNAFUpdate = t2.LastNAFUpdate,
            RegisteredOfficeBool = t2.RegisteredOfficeBool,
            PeriodNumber = t2.PeriodNumber,
            AddressBuildingBlock = t2.AddressBuildingBlock,
            AddressNumber = t2.AddressNumber,
            AddressNumberSubUnit = t2.AddressNumberSubUnit,
            AddressUniqueIdentifier = t2.AddressUniqueIdentifier,
            AddressLabel = t2.AddressLabel,
            AddressPostcode = t2.AddressPostcode,
            AddressMunicipalityLabel = t2.AddressMunicipalityLabel,
            AddressForeignMunicipality = t2.AddressForeignMunicipality,
            AddressPOBox = t2.AddressPOBox,
            AddressCommuneCode = t2.AddressCommuneCode,
            AddressCEDEXCode = t2.AddressCEDEXCode,
            AddressCEDEXLabel = t2.AddressCEDEXLabel,
            AddressOverseasCountryCode = t2.AddressOverseasCountryCode,
            AddressOverseasCountryLabel = t2.AddressOverseasCountryLabel,
            AddressBuildingBlock2 = t2.AddressBuildingBlock2,
            AddressNumber2 = t2.AddressNumber2,
            AddressNumberSubUnit2 = t2.AddressNumberSubUnit2,
            AddressUniqueIdentifier2 = t2.AddressUniqueIdentifier2,
            AddressLabel2 = t2.AddressLabel2,
            AddressPostcode2 = t2.AddressPostcode2,
            AddressMunicipalityLabel2 = t2.AddressMunicipalityLabel2,
            AddressForeignMunicipality2 = t2.AddressForeignMunicipality2,
            AddressPOBox2 = t2.AddressPOBox2,
            AddressCommuneCode2 = t2.AddressCommuneCode2,
            AddressCEDEXCode2 = t2.AddressCEDEXCode2,
            AddressCEDEXLabel2 = t2.AddressCEDEXLabel2,
            AddressOverseasCountryCode2 = t2.AddressOverseasCountryCode2,
            AddressOverseasCountryLabel2 = t2.AddressOverseasCountryLabel2,
            DateOfBusinessStart = t2.DateOfBusinessStart,
            AdministrativeStatus = t2.AdministrativeStatus,
            EstablishmentSign1 = t2.EstablishmentSign1,
            EstablishmentSign2 = t2.EstablishmentSign2,
            EstablishmentSign3 = t2.EstablishmentSign3,
            CommonCompanyName = t2.CommonCompanyName,
            APETCode = t2.APETCode,
            APETCodeCategory = t2.APETCodeCategory,
            EmploymentType = t2.EmploymentType,
            geo_md5 = t2.geo_md5,
            last_modified_date = t2.last_modified_date,
            last_modified_by = t2.last_modified_by""")
            db.commit()

            # get size of etab
            cursor.execute(f'select count(*) from {self.test_etab_table}')
            etab_res = cursor.fetchone()[0]

            # todo need to change md5 gen
            # assert etab_res == staging_res

            break

    def tearDown(self):
        # truncate staging
        cursor.execute('truncate table sirene_stocketab_staging')
        db.commit()

        # remove these tables
        cursor.execute(f'drop table {self.test_geo_location}')
        db.commit()

        cursor.execute(f'drop table {self.test_etab_table}')
        db.commit()
        logger.info(f'end {TestReadParquetEtabStagingToMain.__name__}')


class TestEtabZipCSVToParquet(unittest.TestCase):
    def test(self):
        logger.info(f'beginning {TestEtabZipCSVToParquet.__name__}')
        zip_csv_to_parquet(
            zip_file_path='etab_test.zip',
            parquet_name='_etab_test.parquet',
            schema=etab_schema
        )
        assert '_etab_test.parquet' in os.listdir()

        # so it doesn't mess with other tests
        os.remove('_etab_test.parquet')
        logger.info(f'end {TestEtabZipCSVToParquet.__name__}')


if __name__ == '__main__':
    unittest.main()
    pipeline_messenger(
        title='etab testing has finished',
        text='check logs',
        notification_type='notification'
    )
