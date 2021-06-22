# -*- coding: utf-8 -*-
from setuptools import setup

packages = ["dbt_coves", "dbt_coves.core", "dbt_coves.db", "dbt_coves.tasks", "dbt_coves.utils"]

package_data = {"": ["*"]}

install_requires = [
    "PyYAML>=5.4.1,<6.0.0",
    "SQLAlchemy>=1.3.0,<1.4.0",
    "click>=7.1.2,<8.0.0",
    "luddite>=1.0.1,<2.0.0",
    "packaging>=20.8,<21.0",
    "pretty-errors>=1.2.19,<2.0.0",
    "pydantic>=1.8,<2.0",
    "pyfiglet>=0.8.post1,<0.9",
    "questionary>=1.9.0,<2.0.0",
    "rich>=9.13,<11.0",
    "snowflake-sqlalchemy>=1.2.4",
    "yamlloader>=1.0.0,<2.0.0",
]

entry_points = {"console_scripts": ["dbt-coves = dbt_coves.core.main:main"]}

setup_kwargs = {
    "name": "dbt-coves",
    "version": "0.1.0",
    "description": "CLI tool for dbt users that follow the datacoves guidelines.",
    "long_description": None,
    "author": "Sebastian Sassi",
    "author_email": "ssassi@gmail.com>, Noel Gomez <gomezn@gmail.com>, Michael Kahan <mjkahan23@gmail.com",
    "maintainer": None,
    "maintainer_email": None,
    "url": None,
    "packages": packages,
    "package_data": package_data,
    "install_requires": install_requires,
    "entry_points": entry_points,
    "python_requires": ">=3.7,<3.10",
}


setup(**setup_kwargs)
