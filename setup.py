import setuptools
from setuptools import setup

setup(
    name='haychecker',
    version='0.0.1',
    description='a small library to check for data quality, either with spark or pandas',
    long_description="Description at https://github.com/fruttasecca/hay_checker",
    license='MIT',
    packages=setuptools.find_packages(),
    author='Jacopo Gobbi, Kateryna Konotopska',
    author_email="jacopo.gobbi@studenti.unitn.it, kateryna.konotopska@studenti.unitn.it",
    keywords=["data quality", "completeness", "deduplication", "timeliness", "freshness", "constraint", "rule",
              "entropy", "mutual information", "spark", "pandas"],
    url="https://github.com/fruttasecca/hay_checker",
    classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: MIT License",
        ],
    install_requires=[
        "pyspark>=2.3.1",
        "pandas>=0.23.4",
        "numpy>=1.15.1",
        "scikit-learn>=0.19.2",
        "requests>=2.19.1"
    ]
)
