from dbt.config import Profile, Project
from dbt.config.profile import read_profile, PROFILES_DIR
import dbt.main as dbt
from argparse import ArgumentParser
import os
import sys
from dbtut import *


def map_db_type(dbt_db_name):
    if dbt_db_name == 'postgres':
        return 'postgresql+psycopg2'
    elif dbt_db_name == 'bigquery':
        pass
    elif dbt_db_name == 'snowflake':
        return 'snowflake'
    elif dbt_db_name == 'redshift':
        return 'postgresql+psycopg2'
    else:
        pass


def main():
    args = sys.argv[1:]
    parsed = dbt.parse_args(args)
    project = Project.from_args(parsed)
    profile = Profile.from_args(parsed, project.profile_name)

    """
    due to dbt's usage of popping out values from the profile dictionary we need to parse the yaml again
    popped values include type, threads
    """
    profile_yaml = read_profile(PROFILES_DIR)
    db_type = profile_yaml[profile.profile_name]['outputs'][profile.target_name]['type']

    if profile.credentials['password']:
        db_url = "postgresql+psycopg2://{}:{}@{}/{}".format(
            profile.credentials['user'], profile.credentials['password'],
            profile.credentials['host'], profile.credentials['database'])
    else:
        db_url = "postgresql+psycopg2://{}@{}/{}".format(
            profile.credentials['user'], profile.credentials['host'],
            profile.credentials['database'])

    parser = ArgumentParser()
    parser.add_argument('cmd', help="Option to perform (prepare, run, teardown)")
    parser.add_argument(
        '--projdir', help="Project directory path", default=os.path.curdir)
    parser.add_argument(
        '--sqldump', help="SQL dump file path to create tables",
        default="{}/db/redshift/00_campaign_r/V1.0__campaign_r.sql".format
        (os.path.curdir))
    parser.add_argument(
        '--dataset', help="Dataset and test directory path",
        default="{}/unittest/AttributionAssisted".format(os.path.curdir))
    args = parser.parse_args()

    db_schema_r_conn = connect_db(
        db_url, '{}_r'.format(profile.credentials['schema']))
    db_schema_conn = connect_db(db_url, profile.credentials['schema'])

    if args.cmd == 'prepare':
        prepare_data(db_schema_r_conn, args.dataset, args.sqldump)
    elif args.cmd == 'run':
        test_exec(db_schema_r_conn, db_schema_conn, args.dataset, args.sqldump, args.projdir)
    elif args.cmd == 'cleanup':
        pass


if __name__ == "__main__":
    main()
