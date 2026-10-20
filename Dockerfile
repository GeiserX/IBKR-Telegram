FROM python:3.14-slim

# The base image's ca-certificates 20250419 still trusts the compromised eMudhra
# intermediates; trixie-security carries the release that distrusts them.
RUN apt-get update && apt-get install -y --only-upgrade ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -r app && useradd -r -g app -d /app app

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src/ src/
RUN pip install --no-cache-dir .

RUN chown -R app:app /app
USER app

CMD ["python", "-m", "src"]
