#!/bin/sh

if test -f packages.zip
then 
    echo "remove previous packages.zip"
    rm packages.zip
fi 

pip install -t dependencies -r requirements.txt
cd dependencies; zip -r ../packages.zip .
cd ..; zip -ru packages.zip src -x */__pycache__/\*
rm -rf dependencies

# upload the script and package to target s3 bucket

# submit to emr