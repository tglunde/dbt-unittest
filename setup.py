# -*- coding: utf-8 -*-
#!/usr/bin/env python
from setuptools import find_packages
from distutils.core import setup

package_name = "dbt_ut"
package_version = "0.0.1"
description = """With this dbt Unittesting tool users can 
                 test dbt models written in SQL!"""


setup(
    name=package_name,
    version=package_version,

    description=description,

    author="Torsten Glunde",
    author_email="torsten@glunde.de",
    maintainer="Ilija Kutle",
    maintainer_email="ilija.kutle@alligator-company.com",
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    install_requires=[
        'argparse',
        'ruamel.yaml',
    ],
    entry_points={
        'console_scripts': [
            'dbtut = unittest.test:main',
        ]}
)
