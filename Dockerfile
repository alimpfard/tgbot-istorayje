FROM python:3.10

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends libcurl4-openssl-dev && rm -rf /var/lib/apt/lists/*

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
RUN find /root/nltk_data -name '.gitkeep' -delete && \
    if [ -z "$(ls -A /root/nltk_data)" ]; then python -c "import nltk; nltk.download('stopwords', download_dir='/root/nltk_data'); nltk.download('wordnet', download_dir='/root/nltk_data')"; fi

COPY *.py /app/

ENTRYPOINT ["python", "main.py"]
