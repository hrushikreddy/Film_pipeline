install:
	pip install -r requirements.txt

run_stage1:
	python src/stage1.py data/allFilms.csv data/allFilesSchema.json

run_stage2:
	python src/stage2.py output/allFilms.parquet

run_similarity:
	python src/similarity.py 1 0.2

run_query:
	python src/query_builder.py
