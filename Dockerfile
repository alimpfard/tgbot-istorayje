FROM python:3.10

RUN apt update
RUN apt install libcurl4-openssl-dev

# Cap malloc usage and retention, we don't need a lot of persistent data
ENV MALLOC_ARENA_MAX=2
ENV MALLOC_TRIM_THRESHOLD_=131072

RUN mkdir /app
COPY Pipfile* /app/
COPY *.txt /app/

WORKDIR /app

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY nltk_data/ /root/nltk_data/
RUN if [ -z "$(ls -A /root/nltk_data)" ]; then rm -rf /root/nltk_data && python -c "import nltk; nltk.download('stopwords'); nltk.download('wordnet')"; fi

COPY *.py /app/

ENTRYPOINT ["python", "main.py"]
