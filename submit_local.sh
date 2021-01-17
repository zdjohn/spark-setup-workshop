spark-submit \
    --master local[3] \
    --deploy-mode client \
    --packages=com.amazonaws:aws-java-sdk:1.11.900,org.apache.hadoop:hadoop-aws:3.2.0 \
    --py-files ./dist_files.zip \
    --file settings.json \
    main.py

