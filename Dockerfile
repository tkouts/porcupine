###########################################
# Image for building wheels
###########################################

FROM python:3.8.13-slim-bullseye as build-image

RUN apt-get update
RUN apt-get install -y cmake libffi-dev curl git build-essential cargo

RUN curl -O https://packages.couchbase.com/clients/c/libcouchbase-3.3.0_debian11_bullseye_amd64.tar; \
    tar xf libcouchbase-3.3.0_debian11_bullseye_amd64.tar; \
    cd libcouchbase-3.3.0_debian11_bullseye_amd64; \
    dpkg -i libcouchbase3_3.3.0*.deb libcouchbase-dev*.deb

COPY requirements.txt ./requirements.txt

RUN pip wheel --wheel-dir=/root/wheels --find-links=/root/wheels -r requirements.txt


###########################################
# Image WITHOUT build tools
###########################################

FROM python:3.8.13-slim-bullseye

# copy required libs
COPY --from=build-image /usr/lib/x86_64-linux-gnu/libcouchbase.so.8 /usr/lib/x86_64-linux-gnu

# wheels
COPY --from=build-image /root/wheels /root/wheels

# RUN ls -la /root/wheels

WORKDIR /usr/src/porcupine
COPY porcupine/ ./porcupine/
COPY bin/ ./bin/
ADD requirements.txt .
ADD setup.py .
ADD LICENSE .
ADD README.md .

RUN pip install --no-index --find-links=/root/wheels -r requirements.txt
RUN python setup.py install

# clean-up
RUN rm -rf /root/wheels
RUN rm -rf /usr/src/porcupine

EXPOSE 8000

CMD ["porcupine"]
