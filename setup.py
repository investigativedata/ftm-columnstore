from setuptools import find_packages, setup

with open("README.md") as f:
    long_description = f.read()


setup(
    name="ftm-columnstore",
    version="0.0.5",
    description="Column store implementation for ftm data based on clickhouse",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Simon Wörpel",
    author_email="simon.woerpel@medienrevolte.de",
    url="https://github.com/simonwoerpel/ftm-columnstore",
    license="MIT",
    classifiers=[
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    packages=find_packages(),
    package_dir={"ftm_columnstore": "ftm_columnstore"},
    install_requires=[
        "banal",
        "Click",
        "clickhouse-driver[numpy]",
        "followthemoney",
        "pandas",
        "pyicu",
        "structlog",
    ],
    entry_points={"followthemoney.cli": ["cstore = ftm_columnstore.cli:cli"]},
)
