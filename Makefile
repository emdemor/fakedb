PYTHON ?= python
VENV ?= .venv
POETRY ?= poetry

.PHONY: test test-skip

test:
	@command -v pytest >/dev/null 2>&1 || { \
		echo "pytest not found; install with 'pip install pytest pytest-asyncio'"; \
		exit 1; \
	}
	$(PYTHON) -m pytest -q

test-skip:
	@echo "Running tests that do not require optional dependencies"
	@command -v pytest >/dev/null 2>&1 || { \
		echo "pytest not found; install with 'pip install pytest pytest-asyncio'"; \
		exit 1; \
	}
	$(PYTHON) -m pytest tests/test_fake_postgres.py tests/test_fake_mongo.py -k "plain" -q

