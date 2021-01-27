#!/bin/bash

# export PYSPARK_PYTHON=python3:$PYSPARK_PYTHON
# export PYSPARK_DRIVER_PYTHON=python3:$PYSPARK_DRIVER_PYTHON

printenv

whereis python3

whereis python3.7

aws s3 cp s3://pyspark3-sample/artifact/requirements.txt .
sudo python3 -m pip install -r requirements.txt

python3 -c 'import inspect; import pyspark; print(inspect.getfile(pyspark))'

# sudo python3 -m pip install -U pyspark pyarrow python-dotenv pandas

