FROM ghcr.io/investigativedata/ftm-docker:main

RUN apt-get update && apt-get -y upgrade && apt-get -y autoremove && apt-get clean

COPY ftm_columnstore /app/ftm_columnstore
COPY setup.py /app/setup.py
COPY README.md /app/README.md
COPY pyproject.toml /app/pyproject.toml
COPY VERSION /app/VERSION

WORKDIR /app
RUN pip install --no-cache-dir -U pip setuptools
RUN pip install --no-cache-dir .

ENTRYPOINT ["ftmcs"]
