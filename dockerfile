# python slim image
FROM python:3.12-slim

# set working directory
WORKDIR /app

# create data directory
RUN mkdir -p /app/data

# keep container alive
CMD ["tail", "-f", "/dev/null"]