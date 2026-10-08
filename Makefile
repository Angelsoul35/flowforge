.PHONY: install test lint run demo clean
install:
	pip install -e ".[dev]"
test:
	pytest --cov=flowforge --cov-report=term-missing
lint:
	ruff check flowforge tests
run:
	uvicorn flowforge.main:app --reload --port 8000
demo:
	python -m flowforge.cli demo
clean:
	rm -rf .pytest_cache .ruff_cache .coverage htmlcov
