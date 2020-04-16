FROM ubuntu:bionic

ENV USER_DPA dpa_311
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive && echo "America/Mexico_City" > /etc/timezone && apt-get install -y tzdata

# Instalar comandos utiles
RUN apt-get update && apt-get install -y \
            sudo \
            nano \
            curl \
            git \
            python3-dev \
            python3-pip \
            python3-setuptools \
            libpq-dev && pip3 install --upgrade pip  && pip3 install awscli --upgrade

# creación de usuario genérico
RUN groupadd ${USER_DPA}
RUN useradd dpa_311 -g dpa_311 -m -s /bin/bash
RUN echo 'dpa_311 ALL=(ALL:ALL) NOPASSWD:ALL' | (EDITOR='tee -a' visudo)
RUN echo 'dpa_311:dpa_311' | chpasswd


COPY Pipfile Pipfile.lock ./
RUN pip3 install pipenv && pipenv install --system

USER dpa_311
WORKDIR /home/dpa_311/
