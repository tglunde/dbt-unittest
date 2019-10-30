import xml.etree.ElementTree as ET
import pandas as pd
import sqlalchemy
import pathlib
import os
import time
import subprocess


def get_dataset():
    directory = pathlib.Path("..\\campaign\\unittest\\AttributionAssisted")

    tests = {}
    for row in [x for x in directory.iterdir() if x.is_dir()]:
        tests_table = []
        if row.name not in tests:
            tests[row.name] = tests_table
        else:
            tests_table = tests[row.name]
        for docs in [y for y in row.iterdir()]:
            tests_table.append(docs)
    return tests


def insert_data(document_path, db):
    testdata = {}
    for event, element in ET.iterparse(document_path, events=('start', 'end')):
        if event == 'start' and element.tag != 'dataset':
            testtable = []
            if element.tag not in testdata:
                testdata[element.tag] = testtable
            else:
                testtable = testdata[element.tag]
            testrow = {}
            testtable.append(testrow)
            for attribute in element.attrib.items():
                testrow[attribute[0].lower()] = attribute[1]
    for tablename in testdata.keys():
        df = pd.DataFrame.from_records(
            testdata[tablename],
            columns=testdata[tablename][0].keys())

        df.to_csv('data.csv', index=False)
        df = pd.read_csv('data.csv')
        os.remove('data.csv')

        schema_tablename = tablename.lower().split('.', 1)
        if not db.dialect.has_schema(db, schema_tablename[0]):
            db.execute(sqlalchemy.schema.CreateSchema(schema_tablename[0]))
        df.to_sql(schema_tablename[1], db, schema=schema_tablename[0], if_exists='append', index=False)
    print("{} inserted!".format(pathlib.PurePosixPath(document_path).name))


def recreate_tables(db):
    db.execute("DROP SCHEMA IF EXISTS campaign, campaign_r, campaign_mart, campaign_stg CASCADE; CREATE SCHEMA campaign_r;")

    with open("..\\campaign\\db\\redshift\\00_campaign_r\\V1.0__campaign_r.sql") as sql_dump:
        db.execute(sql_dump.read())
        print("Tables created")


def truncate_db(db):
    meta = sqlalchemy.MetaData(bind=db)
    meta.reflect()
    con = db.connect()
    trans = con.begin()

    for table in meta.sorted_tables:
        con.execute(table.delete())
    trans.commit()
    print("Data truncated!")


def main():
    start_time = time.perf_counter()
    start = time.process_time()

    dbt_project_dir = "..\\campaign"

    db_campaign_r = sqlalchemy.create_engine('postgresql://postgres@localhost/postgres', connect_args={'options': '-csearch_path=campaign_r'})
    db_campaign = sqlalchemy.create_engine('postgresql://postgres@localhost/postgres', connect_args={'options': '-csearch_path=campaign'})
    counter = 0
    dataset = get_dataset()
    test_count = [0, 0, 0]

    recreate_tables(db_campaign_r)
    subprocess.run("dbt run -m stage core", cwd=dbt_project_dir)
    for key, value in dataset.items():
        if key != 'dataset':
            truncate_db(db_campaign)
            if counter < 1:
                for k, v in dataset.items():
                    if k == 'dataset':
                        for file_path in v:
                            if file_path.name.endswith('.xml'):
                                insert_data(file_path, db_campaign_r)
                            elif file_path.name.endswith('.dml'):
                                with open(file_path) as dml_script:
                                    script = dml_script.read()
                                    db_campaign_r.execute(script)
                                    script = ''
                                    print("Init script executed")
                counter += 1
            subprocess.run("dbt run -m core", cwd=dbt_project_dir)
            with open(value[1]) as dml_script:
                script = dml_script.read()
                db_campaign.execute(script)
                script = ''
                print("{} Init script executed".format(key))
            subprocess.run("dbt run -m mart", cwd=dbt_project_dir)
            with open(value[0]) as dml_script:
                script = dml_script.read()
                result = db_campaign.execute(script).fetchall()
                test_count[0] += 1
                for row in result:
                    print(row)
                if len(result) != 0:
                    test_count[2] += 1
                    print('{} TEST FAIL!'.format(key))
                else:
                    test_count[1] += 1
                    print(result)
                    print('{} TEST SUCCESS!'.format(key))
                script = ''
    print('\nTests executed: {} Tests succeded: {} Tests failed: {} \n'.format(test_count[0], test_count[1], test_count[2]))
    print('Execution time: {}'.format(time.perf_counter()-start_time))
    print('CPU execution time: {}'.format(time.process_time()-start))

if __name__ == "__main__":
    main()
