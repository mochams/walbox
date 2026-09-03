.DEFAULT_GOAL := test

.PHONY: test test-unit test-integration test-integration-pgbouncer lint format typecheck run_precommit

test:
	uv run pytest

test-unit:
	uv run pytest tests/unit

test-integration:
	uv run pytest tests/integration -m postgres --cov-fail-under=50

test-integration-pgbouncer:
	uv run pytest tests/integration -m pgbouncer --no-cov

lint:
	uv run ruff check .

format:
	uv run ruff format .

typecheck:
	uv run pyrefly check

run_precommit:
	uv run prek run --all-files
