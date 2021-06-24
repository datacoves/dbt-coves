# -*- coding: utf-8 -*-
from setuptools import setup

packages = \
['dbt_coves',
 'dbt_coves.config',
 'dbt_coves.core',
 'dbt_coves.tasks',
 'dbt_coves.tasks.generate',
 'dbt_coves.ui',
 'dbt_coves.utils']

package_data = \
{'': ['*'], 'dbt_coves': ['templates/*']}

install_requires = \
['Jinja2>=2.11.2,<2.12.0',
 'PyYAML>=5.4.1,<6.0.0',
 'click>=7.1.2,<8.0.0',
 'cookiecutter>=1.7.3,<2.0.0',
 'dbt>=0.19.1,<0.20.0',
 'luddite>=1.0.1,<2.0.0',
 'packaging>=20.8,<21.0',
 'pre-commit>=2.13.0,<3.0.0',
 'pretty-errors>=1.2.19,<2.0.0',
 'pydantic>=1.8,<2.0',
 'pyfiglet>=0.8.post1,<0.9',
 'questionary>=1.9.0,<2.0.0',
 'rich>=10.4.0,<11.0.0',
 'sqlfluff>=0.6.0,<0.7.0',
 'yamlloader>=1.0.0,<2.0.0']

entry_points = \
{'console_scripts': ['dbt-coves = dbt_coves.core.main:main']}

setup_kwargs = {
    'name': 'dbt-coves',
    'version': '0.19.1a4',
    'description': 'CLI tool for dbt users adopting analytics engineering best practices.',
    'long_description': '# dbt-coves\n\n## What is dbt-coves?\n\ndbt-coves is a complimentary CLI tool for [dbt](https://www.getdbt.com/) that allows users to quickly apply [Analytics Engineering](https://www.getdbt.com/what-is-analytics-engineering/) best practices.\n\n### Main features\n\n#### Project initialization\n\n```\ndbt-coves init\n```\n\nInitializes a new ready-to-use dbt project that includes recommended integrations such as [sqlfluff](https://github.com/sqlfluff/sqlfluff), [pre-commit](https://pre-commit.com/), dbt packages, among others.\n\nUses [cookiecutter](https://github.com/cookiecutter/cookiecutter) templates to make it easier to maintain.\n\n#### Models generation\n\n```\ndbt-coves generate <resource>\n```\n\nCode generation tool to easily generate models and model properties based on configuration and existing data.\n\nSupports [Jinja](https://jinja.palletsprojects.com/) templates to adjust how the resources are generated.\n\n#### Quality Assurance\n\n```\ndbt-coves check\n```\n\nRuns a set of checks in your local environment to ensure high quality data.\n\nChecks can be extended by implementing [pre-commit hooks](https://pre-commit.com/#creating-new-hooks).\n\n## Thanks\n\nThe project main structure was inspired by [dbt-sugar](https://github.com/bitpicky/dbt-sugar). Special thanks to [Bastien Boutonnet](https://github.com/bastienboutonnet) for the great work done.\n\n## Authors\n\n- Sebastian Sassi ([@sebasuy](https://twitter.com/sebasuy)) – [Convexa](https://convexa.ai)\n- Noel Gomez ([@noel_g](https://twitter.com/noel_g)) – [Ninecoves](https://ninecoves.com)\n\n## About\n\nLearn more about [Datacoves](https://datacoves.com).\n',
    'author': 'Datacoves',
    'author_email': 'hello@datacoves.com',
    'maintainer': None,
    'maintainer_email': None,
    'url': 'https://github.com/datacoves/dbt-coves',
    'packages': packages,
    'package_data': package_data,
    'install_requires': install_requires,
    'entry_points': entry_points,
    'python_requires': '>=3.7,<3.9',
}


setup(**setup_kwargs)
