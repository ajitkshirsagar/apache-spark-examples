FROM openjdk:8-alpine
LABEL maintainer="AjitKshirsagar"

# Install Python
RUN \
    apk update && \
    apk add --no-cache bash python3 python3-dev py3-pip py3-virtualenv

# Link default Python to Python3
RUN ln -s /usr/bin/python3 /usr/bin/python && \
    ln -s /usr/bin/pip3 /usr/bin/pip

# Install PySpark 
RUN \
    python3 -m pip install --upgrade pip && \
    python3 -m pip install pyspark

# Define working directory
WORKDIR /data

# Define default command
CMD ["bash"]
