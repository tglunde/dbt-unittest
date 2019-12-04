import os
import argparse
from dbtut import test_exec
from ruamel import yaml


def main():

    cfg = yaml.safe_load(open("./config.yml"))
    dbt_proj_dir = cfg['paths']['dbtProjDir']
    dataset_dir = cfg['paths']['datasetDir']
    sql_dump_path = cfg['paths']['sqlDumpPath']

    test_exec(dataset_dir, sql_dump_path, dbt_proj_dir)


if __name__ == "__main__":
    main()
