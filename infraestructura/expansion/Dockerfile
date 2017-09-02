FROM python:3.5-slim

MAINTAINER Adolfo De Unánue <nanounanue@gmail.com>

ENV REFRESHED_AT 2017-01-06

## Actualizamos
RUN \
    apt-get -qq update; apt-get upgrade -y -qq; \
    apt-get install -y --no-install-recommends -qq curl locales bzip2 unzip xz-utils build-essential; \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

## Arreglamos el LOCALE
RUN echo "es_MX.UTF-8 UTF-8" >> /etc/locale.gen \
    && locale-gen es_MX.utf8 \
    && /usr/sbin/update-locale LANG=es_MX.UTF-8

RUN apt-get update && apt-get install -y libsqlite3-dev libmysqlclient-dev libpq-dev

RUN apt-get install -y git \
    && apt-get install -y -qq vim \
    && apt-get install -y wget \
    && apt-get install -y -qq screen

RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt trusty-pgdg main" >> /etc/apt/sources.list' \
    && wget --quiet -O - http://apt.postgresql.org/pub/repos/apt/ACCC4CF8.asc \
    | apt-key add - \
    && apt-get install -y -qq postgresql-9.4-postgis-2.1 pgadmin3 postgresql-contrib

RUN apt-get install -y -qq software-properties-common python-software-properties \
    && add-apt-repository ppa:ubuntugis/ppa -y \
    && apt-get install -y -qq gdal-bin \
    && apt-get install -y -qq libgeos-dev \
    && apt-get clean

RUN wget http://download.osgeo.org/geos/geos-3.4.2.tar.bz2 \
    && tar xjf geos-3.4.2.tar.bz2 \
    && cd geos-3.4.2 \
    && ./configure \
    && make \
    && make install \
    && cd .. \
    && apt-get clean && rm -rf geos-3.4.2/ \
    && rm geos-3.4.2.tar.bz2

RUN mkdir -p /tmp \
    && curl http://download.osgeo.org/gdal/2.1.0/gdal-2.1.0.tar.gz --output gdal-2.1.0.tar.gz --silent \
    && tar zxvf gdal-2.1.0.tar.gz -C /tmp \
    && cd /tmp/gdal-2.1.0/ \
    && ./configure --prefix=/usr/ --with-python -with-geos=yes \
    && make \
    && make install \
    && cd swig/python \
    && python setup.py install \
    && cd ~/.. \
    && apt-get clean && rm -rf /tmp \
    && rm gdal-2.1.0.tar.gz

RUN mkdir -p /tmp \
    && apt-get install -y -qq python-gdal

RUN ln -s /usr/lib/libproj.so.0 /usr/lib/libproj.so
RUN LD_LIBRARY_PATH=/usr/local/lib

RUN git clone https://github.com/geopandas/geopandas.git \
    && cd geopandas \
    && pip install . \
    && cd ~/.. \
    && rm -rf geopandas/

RUN wget http://download.osgeo.org/libspatialindex/spatialindex-src-1.8.5.tar.gz \
    && tar xzvf spatialindex-src-1.8.5.tar.gz \
    && cd spatialindex-src-1.8.5 \ 
    && ./configure \
    && make install \
    && cd .. \
    && rm -rf spatialindex-src-1.8.5 \
    && rm spatialindex-src-1.8.5.tar.gz

RUN git clone https://github.com/jwass/geopandas_osm.git \
    && cd geopandas_osm/ \
    && pip install . \
    && cd .. \
    && rm -rf geopandas_osm/

RUN git clone https://github.com/gboeing/osmnx.git \
    && cd osmnx \
    && pip install . \
    && cd .. \
    && rm -rf osmnx/

RUN apt-get install -y -qq tk-dev && rm -r /var/lib/apt/lists/*

RUN add-apt-repository ppa:ubuntugis/ubuntugis-unstable -y \
    && apt-get install -y -qq postgis

RUN pip install luigi
RUN echo "luigi ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

ADD requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

CMD [ "/bin/sh", "-c", "while true; do echo hello world; sleep 1; done"]

