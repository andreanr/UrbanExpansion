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
    && /usr/sbin/update-locale LANG=es_MX.UTF-8 \
    && dpkg-reconfigure locales

RUN apt-get update && apt-get install -y libsqlite3-dev libmysqlclient-dev

RUN apt-get install -y git

RUN apt-get install -y -qq vim;

RUN apt-get install -y -qq postgresql postgresql-contrib \
    && apt-get install -y -qq libpq-dev

ADD requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

RUN apt-get install -y -qq software-properties-common python-software-properties \
    && add-apt-repository ppa:ubuntugis/ppa \
    && apt-get install -y -qq gdal-bin \
    && apt-get install libgeos-dev \
    && apt-get clean

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

RUN git clone https://github.com/geopandas/geopandas.git \
    && cd geopandas \
    && pip install . \
    && cd ~/.. \
    && rm -rf geopandas/

RUN apt-get install -y -qq wget \
    && wget -O /usr/local/bin/rsub https://raw.github.com/aurora/rmate/master/rmate \
    && chmod a+x /usr/local/bin/rsub \
    && apt-get install -y -qq screen

RUN echo "luigi ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

COPY ../../../UrbanExpansion/ ./UrbanExpansion/
#RUN chmod +x run.sh

EXPOSE 52698

#CMD ["run.sh"]
CMD [ "/bin/sh", "-c", "while true; do echo hello world; sleep 1; done"]

