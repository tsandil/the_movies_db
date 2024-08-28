from setuptools import setup, find_packages

requirements = (
    [
        "sqlalchemy==1.4.53",
        "pandas==2.1.4"
    ]
)

setup(
    name="utilities_etl",
    author="Sandil Tandukar",
    author_email="tan.sandil44@gmail.com",
    install_requires=requirements,
    version="0.0.1",
    packages=find_packages(include=['utilities', 'utilities.*'])
)
