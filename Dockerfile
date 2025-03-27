FROM python:3.12-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    POETRY_VERSION=1.7.1 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_NO_INTERACTION=1

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        build-essential \
        make \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Add Poetry to PATH
ENV PATH="${POETRY_HOME}/bin:$PATH"

# Set working directory
WORKDIR /app

# Copy only the necessary files
COPY pyproject.toml poetry.lock Makefile README.md ./
COPY src/ src/

# Install the project with its dependencies
RUN poetry install --no-dev

# Verify that the package can be imported
RUN poetry run python -c "import dom_collector; print('Package successfully installed')"

# Create directories
RUN mkdir -p snapshots logs

# Set the entrypoint to make with poetry run
ENTRYPOINT ["poetry", "run", "make"] 