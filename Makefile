PYTHON ?= python
export PYTHONPATH := src

MONTH ?= 2024-01

.PHONY: install sample run download run-real test lint clean

install:
	$(PYTHON) -m pip install -r requirements.txt

sample:
	$(PYTHON) -m taxi_pipeline.sample_data --rows 5000 --out data/sample

run:
	$(PYTHON) -m taxi_pipeline.main \
		--input data/sample/yellow_tripdata_sample.csv --input-format csv \
		--zones data/sample/taxi_zone_lookup_sample.csv \
		--output data/curated/sample \
		--period-start 2024-01-01 --period-end 2024-02-01

download:
	bash scripts/download_tlc_data.sh $(MONTH)

run-real:
	$(PYTHON) -m taxi_pipeline.main \
		--input data/raw/yellow_tripdata_$(MONTH).parquet \
		--zones data/raw/taxi_zone_lookup.csv \
		--output data/curated/$(MONTH)

test:
	pytest -v

lint:
	ruff check src tests

clean:
	rm -rf data/curated data/sample spark-warehouse metastore_db derby.log
