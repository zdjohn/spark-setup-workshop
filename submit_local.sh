spark-submit \
    --master local[*] \
    --deploy-mode client \
    --packages=com.amazonaws:aws-java-sdk:1.11.900,org.apache.hadoop:hadoop-aws:3.2.0 \
    --py-files ./packages.zip \
    ./src/run.py

