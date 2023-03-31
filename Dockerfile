FROM ghcr.io/investigativedata/ftm-docker:main

RUN apt-get update && apt-get -y upgrade

COPY ftm_columnstore /app/ftm_columnstore
COPY setup.py /app/setup.py
COPY setup.cfg /app/setup.cfg
COPY VERSION /app/VERSION

WORKDIR /app
RUN pip install -U pip setuptools
RUN pip install .

ENTRYPOINT ["ftmcs"]
