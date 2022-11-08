all: clean install test

install:
	pip install -e .
	pip install twine coverage nose moto pytest pytest-cov black flake8 isort bump2version mypy

test:
	mypy ftm_columnstore/cli.py
	pytest -s --cov=ftm_columnstore --cov-report term-missing

build:
	python setup.py sdist bdist_wheel

prerelease: test
	bump2version patch

release: clean build
	twine upload dist/*

clean:
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clickhouse:  # for testing purposes
	docker run -p 8123:8123 -p 9000:9000 --ulimit nofile=262144:262144 clickhouse/clickhouse-server
