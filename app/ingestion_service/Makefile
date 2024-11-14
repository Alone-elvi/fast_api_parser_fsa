.PHONY: clean venv test-unit test-mock test-integration test-all

PROJECT_ROOT=$(shell pwd)

clean:
	rm -rf venv
	sudo rm -rf .pytest_cache .coverage htmlcov
	sudo find . -type d -name "__pycache__" -exec rm -r {} +
	sudo find . -type f -name "*.pyc" -delete
	sudo find . -type f -name "*.pyo" -delete
	sudo find . -type f -name "*.pyd" -delete
	sudo find . -type f -name ".coverage" -delete
	sudo find . -type d -name "*.egg-info" -exec rm -r {} +
	sudo find . -type d -name "*.egg" -exec rm -r {} + 

venv:
	python3.11 -m venv venv
	venv/bin/pip install --upgrade pip
	venv/bin/pip install -r requirements-dev.txt

test-unit: venv
	PYTHONPATH=. venv/bin/python -m pytest tests/unit -v --cov=. --cov-report=term-missing

test-mock: venv
	PYTHONPATH=. venv/bin/python -m pytest tests/mock -v --cov=. --cov-report=term-missing

test-integration: venv
	PYTHONPATH=. venv/bin/python -m pytest tests/integration -v --cov=. --cov-report=term-missing

test-all: venv
	PYTHONPATH=. venv/bin/python -m pytest tests -v --cov=. --cov-report=term-missing
