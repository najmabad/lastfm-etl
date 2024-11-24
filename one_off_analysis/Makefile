pyspark-notebook-start:
	docker compose -f compose.yaml up --build -d

pyspark-notebook-stop:
	docker compose -f compose.yaml down

pyspark-notebook-teardown:
	docker compose -f compose.yaml down --rmi all
	rm -rf ./data/*