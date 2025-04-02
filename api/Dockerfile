FROM python:3.9-alpine

WORKDIR /app

COPY ./requirements/prod.txt /app/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

COPY ./ctarestapi/ /app/ctarestapi

CMD ["python3", "-m", "ctarestapi.server"]
