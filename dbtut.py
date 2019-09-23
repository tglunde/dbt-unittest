import xml.etree.ElementTree as ET
import pandas as pd
import sqlalchemy
import os
import pathlib


def get_dataset():
    directory = pathlib.Path("./unittest")
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


def insert_data(document_path):
    test_data = {}
    for event, element in ET.iterparse(document_path, events=('start', 'end')):
        if event == 'start' and element.tag!='dataset':
            test_table = []
            if element.tag not in test_data:
                test_data[element.tag] = test_table
            else:
                test_table = test_data[element.tag]
            test_row = {}
            test_table.append(test_row)
            for attribute in element.attrib.items():
                test_row[attribute[0]] = attribute[1]

    db = sqlalchemy.create_engine('postgresql://postgres@localhost/postgres')
    for table_name in test_data.keys():
        df = pd.DataFrame.from_records(
            test_data[table_name], 
            columns=test_data[table_name][0].keys())
        schema_table_name = table_name.split('.', 1)
        if not db.dialect.has_schema(db, schema_table_name[0]):
            db.execute(sqlalchemy.schema.CreateSchema(schema_table_name[0]))
        df.to_sql(schema_table_name[1], db, schema=schema_table_name[0], if_exists='replace', index=False)
    print("{} inserted!".format(pathlib.PurePosixPath(document_path).name))


def main():
    dataset = get_dataset()

    for key, value in dataset.items():
        if key == 'dataset':
            for doc in value:
                if doc.name.endswith('.xml'):
                    insert_data(doc)


if __name__ == "__main__":
    main()
