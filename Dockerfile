FROM python:3.12-slim-bookworm
ARG VERSION=unknown
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 BDC_VERSION=${VERSION}
WORKDIR /app
RUN groupadd --gid 1001 warehouse && useradd --uid 1001 --gid 1001 --no-create-home warehouse
COPY requirements.txt ./
RUN pip install --no-cache-dir --require-hashes -r requirements.txt
COPY worker/ ./worker/
COPY scripts/export_lms_kafka.py scripts/warehouse.py ./scripts/
COPY config/project.json ./config/project.json
COPY contracts/ ./contracts/
COPY ddl/ ./ddl/
COPY transforms/ ./transforms/
COPY checks/ ./checks/
USER 1001:1001
EXPOSE 8080
CMD ["python", "-m", "worker.main"]
