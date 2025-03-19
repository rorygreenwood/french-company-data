import unittest
import os
import unittest
from utils import zip_csv_to_parquet, pipeline_messenger, connect_preprod, logger
from legal_batching import legal_file_schema, read_file_in_chunks, process_batch as legal_process_batch

cursor, db = connect_preprod()


class TestLegalZipCSVToParquet(unittest.TestCase):
    def test(self):
        zip_csv_to_parquet(
            zip_file_path='legal_test.zip',
            parquet_name='_test.parquet',
            schema=legal_file_schema
        )
        assert '_test.parquet' in os.listdir()

        # so it doesn't mess with other tests
        os.remove('_test.parquet')


class TestReadBatchLegal(unittest.TestCase):
    def test(self):
        test_len = 49999
        i = 0
        for file in read_file_in_chunks(file_path='test_legale.parquet', chunk_size=10000):
            i += len(file)

        assert i == test_len


class TestReadParquetLegal(unittest.TestCase):
    def setUp(self):
        cursor.execute('select count(*) from sirene_stocklegal_staging')
        res = cursor.fetchone()[0]
        logger.info(res)

        if res != 0:
            cursor.execute('truncate table sirene_stocklegal')
            db.commit()

    def test(self):
        # find out length of fragment

        for file in read_file_in_chunks(file_path='test_legale.parquet', chunk_size=10000):
            legal_process_batch(df=file, zipfile='test', is_live=False)

            # find out length of fragment
            cursor.execute('select count(*) from sirene_stocklegal_staging')
            res = cursor.fetchone()[0]
            logger.info(res)
            assert res > 0
            cursor.execute("""truncate table sirene_stocklegal_staging""")
            db.commit()
            break

    def tearDown(self):
        cursor.execute('truncate table sirene_stocklegal_staging')
        db.commit()
        logger.info(f'finished {TestReadParquetLegal.__name__}')


class TestReadParquetLegalStagingToMain(unittest.TestCase):
    # create a test table and use this for insert and upsert
    test_legal_table = 'sirene_test_stocklegal'
    test_organisation = 'sirene_test_organisation'
    test_naf = 'sirene_test_naf'

    def setUp(self):
        logger.info(f'start {TestReadParquetLegalStagingToMain.__name__}')
        # try not to affect actual tables
        assert 'test' in self.test_legal_table
        assert 'test' in self.test_organisation
        assert 'test' in self.test_naf

        # create tables for test
        cursor.execute(f'create table if not exists {self.test_legal_table} like sirene_stocklegal')
        db.commit()
        cursor.execute(f'create table if not exists {self.test_organisation} like organisation')
        db.commit()
        cursor.execute(f'create table if not exists {self.test_naf} like naf_code')
        db.commit()

    def test(self):
        for file in read_file_in_chunks(file_path='test_legale.parquet', chunk_size=10000):
            legal_process_batch(df=file, zipfile='test', is_live=False)

            # get size of batch in staging
            cursor.execute('select count(*) from sirene_stocklegal_staging')
            staging_res: int = cursor.fetchone()[0]

            # add upserts to test table here
            cursor.execute(f"""
            insert into {self.test_organisation} (
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
            company_status = sirene_stocklegal_staging.company_status,
            company_type = sirene_stocklegal_staging.company_type,
            last_modified_by = sirene_stocklegal_staging.last_modified_by,
            last_modified_date = sirene_stocklegal_staging.last_modified_date""")
            db.commit()

            cursor.execute(f'select count(*) from {self.test_organisation}')
            legal_res: int = cursor.fetchone()[0]
            # we do not assert if the staging table has 10k values; they are filtered

            assert legal_res == staging_res

            cursor.execute(f"""
            insert into {self.test_naf} (code, organisation_id, name_en, name_fr, last_modified_date, last_modified_by) 

            select distinct NAFCategory, id, t2.name_en, t2.name_fr, last_modified_date, last_modified_by
            from sirene_stocklegal_staging t1

            inner join naf_codes_translations t2
            on t1.NAFCategory = t2.code
            where 
            t1.AdministrativeStatus = 'A'
            and t1.ActiveLegalUnit = 'NAFRev2'
            on duplicate key update last_modified_date = curdate(), last_modified_by = 'test'
            """)
            db.commit()

            # check amount that should be inserted
            cursor.execute("""select count(*) from sirene_stocklegal_staging t1
                        inner join naf_codes_translations t2
            on t1.NAFCategory = t2.code
            where 
            t1.AdministrativeStatus = 'A'
            and t1.ActiveLegalUnit = 'NAFRev2'""")

            naf_pass_res = cursor.fetchone()[0]

            # check length of naf
            cursor.execute(f'select count(*) from {self.test_naf}')
            naf_res = cursor.fetchone()[0]

            assert naf_res == naf_pass_res

            cursor.execute(f"""
            insert into {self.test_legal_table}
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

            cursor.execute(f'select count(*) from {self.test_legal_table}')
            stock_res = cursor.fetchone()[0]

            # ensure size of stock res is equal to staging
            assert stock_res == staging_res
            break

    def tearDown(self):
        logger.info(f'ending {TestReadParquetLegalStagingToMain.__name__}')
        # remove these tables
        cursor.execute(f'drop table {self.test_legal_table}')
        db.commit()

        cursor.execute(f'drop table {self.test_organisation}')
        db.commit()

        cursor.execute(f'drop table {self.test_naf}')
        db.commit()


if __name__ == '__main__':
    unittest.main()
    pipeline_messenger(
        title='legal testing has finished',
        text='check logs',
        notification_type='notification'
    )
