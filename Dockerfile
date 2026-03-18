FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml ./
RUN pip install poetry && poetry config virtualenvs.create false && poetry install --no-dev --no-interaction

COPY . .

EXPOSE 8080

CMD ["python", "-m", "src.serve_labeling", "--port", "8080"]
