#!/bin/bash
source .env
spark-submit \
--conf spark.hadoop.fs.s3a.access.key=$S3_KEY \
--conf spark.hadoop.fs.s3a.secret.key=$S3_SECRET \
--conf spark.hadoop.fs.s3a.endpoint=ip:8080 \
--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
--driver-memory 6G \
--executor-memory 6G \
$@
