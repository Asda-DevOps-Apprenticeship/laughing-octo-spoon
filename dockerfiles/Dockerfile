# Use Python 3.11 as the base image
FROM python:3.11.11 AS base

# Install Poetry and other necessary dependencies
RUN curl -sSL https://install.python-poetry.org | python3 - \
    && apt-get update \
    && apt-get install -y openjdk-17-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Add Poetry to PATH
ENV PATH=$PATH:/root/.local/bin/

# Set up the working directory
WORKDIR /app

# Copy Poetry config files and install dependencies
COPY pyproject.toml poetry.lock ./
RUN poetry install --no-ansi --no-interaction --no-root

# Copy the application code
COPY app /app/app

# Set Spark-related environment variables
ENV PYSPARK_PYTHON=/app/.venv/bin/python
ENV PYSPARK_DRIVER_PYTHON=/app/.venv/bin/python
ENV SPARK_HOME=/opt/spark
ENV PYSPARK_SUBMIT_ARGS="--master local[4] pyspark-shell"

# ==========================
# Testing Stage
# ==========================
FROM base AS test

# Copy .env.test file
COPY .env.test ./

# Run tests with Poetry & pytest
CMD ["poetry", "run", "pytest", "-v", "--disable-warnings"]
