# dbt-unittest
 unit test plugin for dbt 

- column types - 
1. automatic derivation
2. ddl upfront including drop + df.to_sql load/replace

- create schema for tablenames with dot-notation

- include directory parsing to find all dataset-definitions

- script file execution (sql commands from a file)
. sqlalchemy function execute_file

- expected result execution and check wether result set is empty --> positive test result
. return 0 in case of empty result set
. return df with resultset otherwise

- collect TestSuite and Test from directory Structure
. TestSuite has a setup method (xml-datasets + init.dml) and a list of Tests
. Test has an init.dml and expected.dml
. TestResult --> list of TestSuites/Tests and a repot of green/red tests with error messages

- build it into dbt