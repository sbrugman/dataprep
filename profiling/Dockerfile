FROM python:3.7

WORKDIR /workdir

RUN apt update && apt install -y libsnappy-dev

COPY build/requirements.txt /workdir

RUN pip install -r /workdir/requirements.txt

RUN pip install -U ipython pyarrow python-snappy dask[complete] pandas-profiling