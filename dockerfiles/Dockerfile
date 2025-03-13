FROM python:3.11 AS base

RUN curl -sSL https://install.python-poetry.org | python3 - \ 
    && apt-get update \
    && apt-get upgrade -y \
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

# Add Poetry to PATH
ENV PATH=$PATH:/root/.local/bin/

WORKDIR /app

COPY pyproject.toml poetry.lock ./

COPY app /app/app

# Install dependencies
RUN poetry install  --no-ansi --no-interaction

FROM python:3.11  AS test

COPY --from=base /root /root
ENV PATH=$PATH:/root/.local/bin/
WORKDIR /app

COPY --from=base /app /app
# COPY .env.test ./

ENTRYPOINT ["poetry", "run", "pytest"]



# FROM python:3.12-slim AS prod

# ENV FLASK_DEBUG=false
# COPY --from=base /root /root
# ENV PATH=$PATH:/root/.local/bin/
# WORKDIR /app
# COPY --from=base /app /app

# ENTRYPOINT poetry run  flask db upgrade  && flask run --host 0.0.0.0
