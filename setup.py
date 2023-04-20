#!/usr/bin/env python
from setuptools import setup

with open("README.md", "r", encoding="utf-8") as filehandler:
    long_description = filehandler.read()

setup(
    name="tap-stay-ai",
    version="0.1.0",
    description="Singer.io tap for extracting data from Stay AI's Open API",
    long_description=long_description,
    long_description_content_type='text/markdown',
    author="Maldrotic",
    url="https://github.com/Maldrotic/tap-stay-ai",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_stay_ai"],
    install_requires=[
        "singer-python==5.13.0",
        "requests==2.28.2",
    ],
    entry_points="""
    [console_scripts]
    tap-stay-ai=tap_stay_ai:main
    """,
    packages=["tap_stay_ai"],
    package_data={
        "schemas": ["tap_stay_ai/schemas/*.json"]
    },
    include_package_data=True,
)
