from setuptools import find_packages, setup

setup(
    name="factory",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "dagster_duckdb_pandas",
        "dagster_duckdb",
        "dagster-cloud",
        "Faker==18.4.0",
        "matplotlib",
        "pandas",
        "numpy",
        "requests"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
