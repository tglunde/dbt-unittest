import xml.etree.ElementTree as ET
import pandas as pd
import sqlalchemy
import pathlib
import os
import time
import subprocess


def get_dataset(dataset_dir):
    data_and_test = pathlib.Path(dataset_dir)

    data_tests = {}
    for directory in [x for x in data_and_test.iterdir() if x.is_dir()]:
        tests_table = []
        if directory.name not in data_tests:
            data_tests[directory.name] = tests_table
        else:
            tests_table = data_tests[directory.name]
        for docs in [y for y in directory.iterdir()]:
            tests_table.append(docs)
    return data_tests


def insert_xml_data(document_path, db):
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

        schema_tablename = tablename.lower().split('.', 1)
        if not db.dialect.has_schema(db, schema_tablename[0]):
            db.execute(sqlalchemy.schema.CreateSchema(schema_tablename[0]))
        df.to_sql(
            schema_tablename[1], db, schema=schema_tablename[0],
            if_exists='append', index=False)
    print("{} inserted!".format(pathlib.PurePosixPath(document_path).name))


def recreate_tables(db, sql_dump_path):
    db.execute("DROP SCHEMA IF EXISTS campaign, campaign_r, campaign_mart, \
        campaign_stg CASCADE; CREATE SCHEMA campaign_r;")

    with open(sql_dump_path) as sql_dump:
        db.execute(sql_dump.read())
        print("Tables created")


def truncate_db(db):
    meta = sqlalchemy.MetaData(bind=db)
    meta.reflect()
    con = db.connect()

    for table in meta.tables:
        con.execute('TRUNCATE TABLE ' + table)
    print("Data truncated!")


def insert_data(dataset, db_schema_r):
    for directory, files in dataset.items():
        if directory == 'dataset':
            for file_path in files:
                if file_path.name.endswith('.xml'):
                    insert_xml_data(file_path, db_schema_r)
                elif file_path.name.endswith('.dml'):
                    with open(file_path) as dml_script:
                        script = dml_script.read()
                        db_schema_r.execute(script)
                        script = ''
                        print("Init script executed")


def prepare_data(db_schema_r, dataset_dir, sql_dump_path):
    recreate_tables(db_schema_r, sql_dump_path)
    dataset = get_dataset(dataset_dir)
    insert_data(dataset, db_schema_r)


def connect_db(uri, schema_name):
    return sqlalchemy.create_engine(
        uri, connect_args={'options': '-csearch_path=' + schema_name})


def test_exec(db_schema_r, db_schema, dataset_dir, sql_dump_path, dbt_proj_dir):
    start_time = time.perf_counter()
    start = time.process_time()

    counter = 0
    dataset = get_dataset(dataset_dir)
    test_count = [0, 0, 0]

    recreate_tables(db_schema_r, sql_dump_path)
    subprocess.run("dbt run -m stage core", cwd=dbt_proj_dir)

    for key, value in dataset.items():
        if key != 'dataset':
            truncate_db(db_schema)
            if counter < 1:
                insert_data(dataset, db_schema_r)
                counter += 1
            subprocess.run("dbt run -m core", cwd=dbt_proj_dir)
            with open(value[1]) as init_dml:
                script = init_dml.read()
                db_schema.execute(script)
                script = ''
                print("{} Init script executed".format(key))
            subprocess.run("dbt run -m mart", cwd=dbt_proj_dir)
            with open(value[0]) as expected_dml:
                script = expected_dml.read()
                result = db_schema.execute(script).fetchall()
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

    print('\nTests executed: {} Tests succeded: {} Tests failed: {} \n'.format(
        test_count[0], test_count[1], test_count[2]))

    print('Execution time: {}'.format(time.perf_counter()-start_time))
    print('CPU execution time: {}'.format(time.process_time()-start))
