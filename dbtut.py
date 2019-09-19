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
        df.to_sql(tablename, db, if_exists='replace',)
    

if __name__ == "__main__":
    main()
