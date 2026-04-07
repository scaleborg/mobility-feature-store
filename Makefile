PYTHON=python3

setup:
	$(PYTHON) -m venv .venv
	. .venv/bin/activate && pip install -e ".[dev]"

test:
	pytest -q

lint:
	ruff check .
	black --check .

format:
	black .
	ruff check --fix .

run-api:
	uvicorn api.main:app --reload --port 8000

register-feature-views:
	PYTHONPATH=. $(PYTHON) scripts/register_feature_views.py

generate-synthetic:
	PYTHONPATH=. $(PYTHON) scripts/generate_synthetic_source.py

materialize:
	PYTHONPATH=. $(PYTHON) scripts/materialize.py

validate:
	$(PYTHON) scripts/validate.py

refresh-latest:
	$(PYTHON) scripts/refresh_latest.py
