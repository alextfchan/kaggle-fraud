# Base
FROM python:3.11-slim-bookworm AS python-base

LABEL maintainer="A"
LABEL description="Python basic ETL"
LABEL build_date="2025-05-13"

# Set Poetry Environment
ENV POETRY_VERSION=2.0.1
# ENV POETRY_HOME=/opt/poetry
ENV POETRY_VENV=/opt/poetry-venv
ENV POETRY_CACHE_DIR=/opt/.cache


# Create a stage for Poetry installation
FROM python-base AS poetry-base

# Install Poetry
RUN python -m venv $POETRY_VENV \
    && $POETRY_VENV/bin/pip install -U pip setuptools \
    && $POETRY_VENV/bin/pip install poetry==${POETRY_VERSION}


# Create a stage for Pipeline
FROM python-base AS pipeline-base

# Add Poetry to PATH
COPY --from=poetry-base ${POETRY_VENV} ${POETRY_VENV}
ENV PATH="${PATH}:${POETRY_VENV}/bin"

# Set working directory
WORKDIR /pipeline
ENV PYTHONPATH="${PYTHONPATH}:/pipeline/"

# Copy Dependencies
COPY poetry.lock pyproject.toml ./
COPY README.md ./

# Install dependencies (avoiding packaging project)
RUN poetry check
RUN poetry install --no-root 

# Copy Project Files
COPY . /pipeline

# Running Pipeline
CMD ["poetry", "run", "python", "./src/pipeline.py"]
