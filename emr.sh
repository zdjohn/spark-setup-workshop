# package 
tox -e pack

# upload to s3 
aws s3 cp dist s3://pyspark3-sample/artifact/ --recursive

#involke
aws emr create-cluster --name "review-clean-step" \
    --release-label emr-6.1.0 \
    --applications Name=Spark \
    --log-uri s3://pyspark3-sample/logs/ \
    --ec2-attributes KeyName=emr-sample-keypair \
    --instance-type c5.xlarge \
    --instance-count 3 \
    --configurations '[{"Classification":"spark-env","Configurations":[{"Classification":"export","Properties":{"PYSPARK_PYTHON":"/usr/bin/python3"}}]}]' \
    --bootstrap-action Path="s3://pyspark3-sample/artifact/bootstrap.sh" \
    --steps Type=Spark,Name="s-job-1",ActionOnFailure=TERMINATE_CLUSTER,Args=[--deploy-mode,cluster,--master,yarn,--py-files,s3://pyspark3-sample/artifact/dist_files.zip,--files,s3://pyspark3-sample/artifact/settings.json,s3://pyspark3-sample/artifact/main.py,--job=cross_domain,--source_domain=Digital_Video_Download,--target_domain=Digital_Music_Purchase] \
    --use-default-roles \
    --auto-terminate