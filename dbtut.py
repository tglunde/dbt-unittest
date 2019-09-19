import xml.etree.ElementTree as ET
import pandas as pd
import sqlalchemy

def main():
    document_path = "./unittest/dataset/customerlayer_functional_testset_campaign_r.xml"
    testdata = {}
    for event, element in ET.iterparse(document_path, events=('start', 'end')):
        if event == 'start' and element.tag!='dataset':
            testtable = []
            if element.tag not in testdata:
                testdata[element.tag] = testtable
            else:
                testtable = testdata[element.tag]
            testrow = {}
            testtable.append(testrow)
            for attribute in element.attrib.items():
                testrow[attribute[0]] = attribute[1]

    db = sqlalchemy.create_engine('postgresql://postgres@localhost/postgres')
    for tablename in testdata.keys():
        df = pd.DataFrame.from_records(
            testdata[tablename], 
            columns=testdata[tablename][0].keys())
        schema_tablename = tablename.split('.', 1)
        if not db.dialect.has_schema(db, schema_tablename[0]):
            db.execute(sqlalchemy.schema.CreateSchema(schema_tablename[0]))
        df.to_sql(schema_tablename[1], db, schema=schema_tablename[0], if_exists='replace', index=False)
    print("{} inserted!".format(pathlib.PurePosixPath(document_path).name))
    

if __name__ == "__main__":
    main()
