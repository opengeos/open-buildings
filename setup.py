#!/usr/bin/env python

"""The setup script."""

import io
from os import path as op
from setuptools import setup, find_packages

with open('README.md', encoding="utf-8") as readme_file:
    readme = readme_file.read()

here = op.abspath(op.dirname(__file__))

# get the dependencies and installs
with io.open(op.join(here, "requirements.txt"), encoding="utf-8") as f:
    all_reqs = f.read().split("\n")

install_requires = [x.strip() for x in all_reqs if "git+" not in x]
dependency_links = [x.strip().replace("git+", "") for x in all_reqs if "git+" not in x]

requirements = ['Click>=7.0', ]

setup_requirements = [ ]

test_requirements = [ ]

setup(
    author="Chris Holmes",
    author_email='cholmes@9eo.org',
    python_requires='>=3.8',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
    description="Tools for working with open building datasets",
    entry_points={
        'console_scripts': [
            'ob=open_buildings.cli:main',
        ],
    },
    install_requires=install_requires,
    dependency_links=dependency_links,
    license="Apache Software License 2.0",
    long_description=readme,
    long_description_content_type='text/markdown',
    include_package_data=True,
    keywords='open_buildings',
    name='open-buildings',
    packages=find_packages(),
    package_data={
        'open_buildings': ['google/*', 'overture/*' ],
    },
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/opengeos/open-buildings',
    version='0.10.0',
    zip_safe=False,
)
