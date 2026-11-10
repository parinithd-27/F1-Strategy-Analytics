# App + Spark image: Java 17 (Temurin, Ubuntu 24.04) + Python 3.12 + locked deps via uv,
# with the spark-sql-kafka connector pre-installed so Structured Streaming works offline.
FROM eclipse-temurin:17-jdk-noble

ARG UV_VERSION=0.13.7
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3.12 python3.12-venv curl procps tini \
 && rm -rf /var/lib/apt/lists/* \
 && python3.12 -m venv /opt/uv-bootstrap \
 && /opt/uv-bootstrap/bin/pip install --no-cache-dir "uv==${UV_VERSION}" \
 && ln -s /opt/uv-bootstrap/bin/uv /usr/local/bin/uv

WORKDIR /opt/f1sa
# Dependencies first (cached layer), then the project itself.
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --locked --no-dev --no-install-project --python /usr/bin/python3.12
COPY src ./src
RUN uv sync --locked --no-dev --no-editable --python /usr/bin/python3.12

ENV PATH=/opt/venv/bin:$PATH \
    SPARK_HOME=/opt/venv/lib/python3.12/site-packages/pyspark \
    PYSPARK_PYTHON=/opt/venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python
ENV PATH=$SPARK_HOME/bin:$PATH

COPY docker/spark/fetch_kafka_jars.py /opt/f1sa/docker/fetch_kafka_jars.py
RUN python /opt/f1sa/docker/fetch_kafka_jars.py \
 && python /opt/f1sa/docker/fetch_kafka_jars.py --verify \
 && ls "$SPARK_HOME/jars" | grep -E 'spark-sql-kafka-0-10_2.13-4.2.1.jar|kafka-clients'

# Run as a non-root user whose UID matches the host user, so files written to the bind-mounted
# data/ and results/ dirs stay owned by the developer. (Ubuntu images ship 'ubuntu' as UID 1000.)
ARG APP_UID=1000
RUN (userdel --remove ubuntu 2>/dev/null || true) \
 && useradd --create-home --uid "${APP_UID}" --shell /bin/bash f1sa \
 && mkdir -p /tmp/spark-work /tmp/spark-events && chown f1sa /tmp/spark-work /tmp/spark-events
USER f1sa
WORKDIR /workspace

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["sleep", "infinity"]
