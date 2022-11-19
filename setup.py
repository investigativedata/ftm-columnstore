from setuptools import find_packages, setup

with open("README.md") as f:
    long_description = f.read()


setup(
    name="ftm-columnstore",
    version="0.0.6",
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
    package_data={"ftm_columnstore": ["py.typed"]},
    install_requires=[
        "banal",
        "Click",
        "clickhouse-driver[numpy]",
        "followthemoney",
        "nomenklatura",
        "pandas",
        "pyicu",
        "structlog",
        "libindic-soundex",
        "libindic-utils",
        "metaphone",
    ],
    extras_require={
        "predict": [
            "followthemoney_typepredict @ git+https://github.com/alephdata/followthemoney-typepredict.git",
        ],
    },
    entry_points={"console_scripts": ["ftmcs = ftm_columnstore.cli:cli"]},
)
