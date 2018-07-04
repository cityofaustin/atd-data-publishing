FROM python:3.6

#  Set the working directory
WORKDIR /app

#  Copy package requirements
COPY requirements.txt /app

RUN apt-get update
RUN apt-get install dialog apt-utils -y

#  Install tzdata and set timezone
RUN apt-get install -y tzdata
ENV TZ=America/Chicago
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

#  Install cifs-utils to mount Windows network share
RUN apt-get install -y cifs-utils

RUN apt-get update --fix-missing
RUN apt-get install -y iputils-ping

#  Required for pymssql
RUN apt-get update && apt-get install -y \
    freetds-bin \
    freetds-common \
    freetds-dev

#  Update python3-pip
RUN python -m pip install pip --upgrade
RUN python -m pip install wheel

#  Install python packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt