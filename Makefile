PYTHON ?= python
VENV ?= .venv
POETRY ?= poetry

.PHONY: test test-skip coverage build publish

test:
	@command -v pytest >/dev/null 2>&1 || { \
		echo "pytest not found; install with 'pip install pytest pytest-asyncio'"; \
		exit 1; \
	}
	$(PYTHON) -m pytest -q --cov=fakedb --cov-report=term-missing

test-skip:
	@echo "Running tests that do not require optional dependencies"
	@command -v pytest >/dev/null 2>&1 || { \
		echo "pytest not found; install with 'pip install pytest pytest-asyncio'"; \
		exit 1; \
	}
	$(PYTHON) -m pytest tests/test_fake_postgres.py tests/test_fake_mongo.py -k "plain" -q

coverage:
	@command -v pytest >/dev/null 2>&1 || { \
		echo "pytest not found; install with 'pip install pytest pytest-asyncio pytest-cov'"; \
		exit 1; \
	}
	$(PYTHON) -m pytest --cov=fakedb --cov-report=term-missing

build:
	@$(PYTHON) -m pip show build >/dev/null 2>&1 || { \
		echo "build not found; install with 'pip install build'"; \
		exit 1; \
	}
	@$(PYTHON) -m pip show twine >/dev/null 2>&1 || echo "Tip: install twine with 'pip install twine' for uploads"
	rm -rf build dist
	$(PYTHON) -m build

publish: build
	@$(PYTHON) -m pip show twine >/dev/null 2>&1 || { \
		echo "twine not found; install with 'pip install twine'"; \
		exit 1; \
	}
	$(PYTHON) -m twine upload dist/*
