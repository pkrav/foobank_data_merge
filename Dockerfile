FROM python:3.8.0-slim

ARG BUCKET
ENV BUCKET=${BUCKET}

ARG GOOGLE_AUTH_CODE
ENV GOOGLE_AUTH_CODE=${GOOGLE_AUTH_CODE}

RUN mkdir /main
COPY ["merge_data.py", "requirements.txt", "/main/"]

RUN pip3 install -r /main/requirements.txt

WORKDIR /main

ENTRYPOINT python /main/merge_data.py --bucket ${BUCKET} --auth ${GOOGLE_AUTH_CODE}
