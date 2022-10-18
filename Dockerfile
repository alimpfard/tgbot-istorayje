FROM python:3.10

RUN apt update
RUN apt install libcurl4-openssl-dev

RUN mkdir /app
COPY Pipfile* /app/
COPY *.py /app/
COPY *.txt /app/

WORKDIR /app

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

RUN python -c "import nltk; nltk.download('stopwords')"

ENTRYPOINT ["python", "main.py"]
