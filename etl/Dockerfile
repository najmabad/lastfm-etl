FROM docker.io/bitnami/spark:3.5

USER root
RUN install_packages pipenv

COPY config/log4j2.properties /opt/bitnami/spark/conf/log4j2.properties

WORKDIR /app

COPY Pipfile Pipfile.lock top_songs.py ./
COPY config /app/config
COPY data /app/data
COPY etl_tasks /app/etl_tasks
COPY etl_utils /app/etl_utils


# install all the prod packages at the system level
RUN pipenv install --system --deploy --ignore-pipfile

USER 1001