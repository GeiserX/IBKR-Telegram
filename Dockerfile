FROM python:3.14-slim

RUN groupadd -r app && useradd -r -g app -d /app app

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src/ src/
RUN pip install --no-cache-dir .

RUN chown -R app:app /app
USER app

CMD ["python", "-m", "src"]
