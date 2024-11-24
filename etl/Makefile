spark-cluster-start:
	docker compose -f compose.yaml up --build -d

submit-top-songs-app:
	docker exec spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client /app/top_songs.py \
	--config $(if $(CONFIG),$(CONFIG), config/default.json) \
	--session-threshold $(if $(SESSION_THRESHOLD),$(SESSION_THRESHOLD), 20) \
	--session-number $(if $(SESSION_NUMBER),$(SESSION_NUMBER), 50) \
	--song-number $(if $(SONG_NUMBER),$(SONG_NUMBER), 10)

spark-cluster-stop:
	docker compose -f compose.yaml down

deploy-app:
	make spark-cluster-start
	make submit-top-songs-app
	make spark-cluster-stop

test-spark-cluster-start:
	docker compose -f compose.test.yaml up --build -d

test-execute:
	docker exec test-runner-master pytest $(TEST)

test-spark-cluster-stop:
	docker compose -f compose.test.yaml down

test-run:
	make test-spark-cluster-start
	make test-execute
	make test-spark-cluster-stop

teardown-all:
	docker compose -f compose.yaml down --rmi all
	docker compose -f compose.test.yaml down --rmi all
	rm -rf ./data/*