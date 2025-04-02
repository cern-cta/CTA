FROM python:3.9-alpine

WORKDIR /app

COPY ./requirements/prod.txt /app/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

COPY ./src/ /app/src

CMD ["python3", "src/main.py"]
