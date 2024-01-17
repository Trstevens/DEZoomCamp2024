FROM python:3.9.1

RUN pip install pandas

WORKDIR /app
COPY pipeline.py pipeline.py

ENTRYPOINT [ "python", "pipeline.py" ]


# docker run -it \
#     -e POSTGRES_USER="root" \
#     -e POSTGRES_PASSWORD="root" \
#     -e POSTGRES_DB="ny_taxi" \
#     -v dtc_postgres_volume_local:/var/lib/postgresql/data \
#     -p 5432:5432 \
#     --network=pg-network \
#     --name pg-database \ 
#     postgres:13

# docker run -it \
#   -e POSTGRES_USER="root" \
#   -e POSTGRES_PASSWORD="root" \
#   -e POSTGRES_DB="ny_taxi" \
#   -v dtc_postgres_volume_local:/var/lib/postgresql/data \
#   -p 5432:5432 \
#   postgres:13