###########################################
# Image for building wheels
###########################################

FROM python:3.8.10-alpine3.13 as build-image

RUN echo "https://dl-3.alpinelinux.org/alpine/v3.13/main" >> /etc/apk/repositories
RUN echo "https://dl-3.alpinelinux.org/alpine/v3.13/community" >> /etc/apk/repositories
RUN apk update

RUN apk add --no-cache linux-headers g++ gcompat libgcc make cmake libffi-dev
RUN apk add --no-cache libcouchbase-dev
# for building pendulum/orjson
RUN apk add --no-cache cargo

COPY requirements.txt ./requirements.txt

# orjson wheel build
RUN pip install "pip==20.2.2" --user &&\
    echo 'manylinux2014_compatible = True;' > /usr/local/lib/python3.8/_manylinux.py &&\
    pip wheel --wheel-dir=/root/wheels orjson==3.5.3 &&\
    rm /usr/local/lib/python3.8/_manylinux.py &&\
    mv /root/wheels/orjson-3.5.3-cp38-cp38-manylinux_2_17_x86_64.manylinux2014_x86_64.whl /root/wheels/orjson-3.5.3-py3-none-any.whl &&\
    pip uninstall -y pip

RUN pip wheel --wheel-dir=/root/wheels --find-links=/root/wheels -r requirements.txt


###########################################
# Image WITHOUT build tools
###########################################

FROM python:3.8.10-alpine3.13

# copy required libs
COPY --from=build-image /usr/lib/libcouchbase.so.2 /usr/lib/
COPY --from=build-image /usr/lib/libstdc++.so.6 /usr/lib/
COPY --from=build-image /usr/lib/libgcc_s.so.1 /usr/lib/

# wheels
COPY --from=build-image /root/wheels /root/wheels

RUN ls -la /root/wheels

# required by orjson
RUN apk add --no-cache gcompat
RUN apk add --no-cache tzdata
RUN cp /usr/share/zoneinfo/UTC /etc/localtime
RUN echo "UTC" >  /etc/timezone

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
