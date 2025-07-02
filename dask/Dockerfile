FROM daskdev/dask:2022.10.0-py3.10


ENV DEBIAN_FRONTEND=noninteractive


RUN apt update && \
    apt install -y curl poppler-utils libgl1 libglib2.0-0 tzdata && \
    rm -rf /var/lib/apt/lists/*


#RUN apt-get install -y tzdata

WORKDIR /dask

# Inject AWS credentials as environment variables
ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY
ARG AWS_DEFAULT_REGION
ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
ENV AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
ENV AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION


COPY  dask/requirements.txt /app/requirements.txt

RUN pip install -r /app/requirements.txt


COPY ./preprocessing /dask/preprocessing


# Add the directory to the Python path so Dask workers can import it
ENV PYTHONPATH="/dask/preprocessing:${PYTHONPATH}"
ENV DASK_DATAFRAME__CONVERT_STRING=False



ENV TZ=UTC

EXPOSE 8787
EXPOSE 8786