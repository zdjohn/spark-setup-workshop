#!/bin/sh

if [ -fe packages.zip ]
then 
    rm packages.zip
fi 

pip install -t dependencies -r requirements.txt
cd dependencies; zip -r ../packages.zip .
cd ..; zip -ru packages.zip commons -x commons/__pycache__/\*
rm -rf dependencies