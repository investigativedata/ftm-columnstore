install:
	poetry install --with dev

lint:
	poetry run flake8 ftm_columnstore --count --select=E9,F63,F7,F82 --show-source --statistics
	poetry run flake8 ftm_columnstore --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics

pre-commit:
	poetry run pre-commit install
	poetry run pre-commit run -a

typecheck:
	poetry run mypy --strict ftm_columnstore

test:
	poetry run pytest tests -v --capture=sys --cov=ftm_columnstore --cov-report term-missing

build:
	poetry run build

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
