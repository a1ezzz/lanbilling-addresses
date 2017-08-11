FROM debian
MAINTAINER Ildar Gafurov <dev@binblob.com>

ENV WASP_GENERAL_REVISION 59f2a8e3b2cc2d88926a81cb3b2a292021e7784a
ENV WASP_LAUNCHER_REVISION fe405105e5d53dd7fac73363426b52402b848c02
ENV LANBILLING_ADDRESSES_REVISION 08d7d8710c4c33b752ca0b3c12c5c2a641b9d5b6

ENV FIAS_URL http://fias.nalog.ru/Public/Downloads/20170626/fias_xml.rar
ENV FIAS_FILE fias_xml.rar

# Docker IP-address must be in Lanbilling trusted list
ENV LABILLING_LOGIN=''
ENV LABILLING_PASSWORD=''
ENV LABILLING_HOSTNAME=''
ENV LABILLING_WSDL_URL=''
ENV LABILLING_SOAP_PROXY_ADDRESS=''
ENV LABILLING_LIMIT_REGIONS=''

RUN apt-get update
RUN apt-get upgrade -y

RUN apt-get install -y mongodb
RUN apt-get install -y git
RUN apt-get install -y python3-pip
RUN apt-get install -y wget
RUN echo "deb http://deb.debian.org/debian stable non-free" >> /etc/apt/sources.list
RUN apt-get update
RUN apt-get install -y unrar

RUN useradd -m lanbilling_exporter
WORKDIR /home/lanbilling_exporter

RUN git clone https://github.com/a1ezzz/wasp-general
WORKDIR /home/lanbilling_exporter/wasp-general
RUN git checkout $WASP_GENERAL_REVISION
RUN pip3 install -r /home/lanbilling_exporter/wasp-general/requirements.txt
RUN pip3 install /home/lanbilling_exporter/wasp-general

WORKDIR /home/lanbilling_exporter
RUN git clone https://github.com/a1ezzz/wasp-launcher
WORKDIR /home/lanbilling_exporter/wasp-launcher
RUN git checkout $WASP_LAUNCHER_REVISION
RUN pip3 install -r /home/lanbilling_exporter/wasp-launcher/requirements.txt
RUN pip3 install /home/lanbilling_exporter/wasp-launcher

WORKDIR /home/lanbilling_exporter
RUN git clone https://github.com/a1ezzz/lanbilling-addresses
WORKDIR /home/lanbilling_exporter/lanbilling-addresses
RUN git checkout $LANBILLING_ADDRESSES_REVISION
RUN pip3 install -r /home/lanbilling_exporter/lanbilling-addresses/requirements.txt
RUN pip3 install /home/lanbilling_exporter/lanbilling-addresses

RUN mkdir /fias-data
WORKDIR /fias-data
RUN wget $FIAS_URL -O /fias-data/$FIAS_FILE
RUN unrar x $FIAS_FILE 'AS_ADDROBJ*'

RUN apt-get install -y locales
RUN locale-gen ru_RU.UTF-8
ENV LC_ALL ru_RU.UTF-8
ENV LANG ru_RU.UTF-8

RUN echo "ru_RU.UTF-8 UTF-8" >> /etc/locale.gen
RUN dpkg-reconfigure --frontend=noninteractive locales
RUN update-locale LANG=ru_RU.UTF-8

WORKDIR /home/lanbilling_exporter
EXPOSE 7220
ENTRYPOINT service mongodb start && /bin/bash /home/lanbilling_exporter/lanbilling-addresses/docker/launcher_run.sh
