PYTHON ?= python
VENV ?= .venv
POETRY ?= poetry

.PHONY: test test-skip coverage

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
