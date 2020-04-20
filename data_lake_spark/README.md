Steps:
1. Get a sense of the data locally by running pyspark shell locally or setup jupyter locally
2. Run same code on EMR notebook with S3 paths and see that the code can handle large dataset efficiently
3. Modify the script to reflect the tested notebook code and run it on EMR cluster

Notes:
Spark installation guide for Ubuntu:
https://datawookie.netlify.app/blog/2017/07/installing-spark-on-ubuntu/
Interesting link on how Jupyter kernels are defined and how to define one for pyspark:
https://anchormen.nl/blog/big-data-services/pyspark-jupyter-kernels/
I decided to create a kernel for Pyspark:
```
{
  "display_name": "PySpark",
  "language": "python",
  "argv": [ "/home/rvdubey/workspace/data_eng_nanodegree/data_eng_env/bin/python", "-m", "ipykernel", "-f", "{connection_file}" ],
  "env": {
    "SPARK_HOME": "/usr/local/spark",
    "PYSPARK_PYTHON": "/home/rvdubey/workspace/data_eng_nanodegree/data_eng_env/bin/python3",
    "PYTHONPATH": "/usr/local/spark/python/:/usr/local/spark/python/lib/py4j-0.10.7-src.zip",
    "PYTHONSTARTUP": "/usr/local/spark/python/pyspark/shell.py",
    "PYSPARK_SUBMIT_ARGS": "--master local[2] pyspark-shell"
  }
}
```
Set this PYSPARK_PYTHON in /usr/local/spark/spark-env.sh as well so that it has access to the virtualenv-python

Speeding up S3 writes on EMR:
https://aws.amazon.com/blogs/big-data/improve-apache-spark-write-performance-on-apache-parquet-formats-with-the-emrfs-s3-optimized-committer/
https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-s3-optimized-committer.html

To run the script:
$SPARK_HOME/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:2.7.0 etl.py
```
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.access.key", access_id)
hadoop_conf.set("fs.s3a.secret.key", access_key)
```
https://github.com/jupyter/enterprise_gateway/issues/523

Issues with not being able to install dependencies:
https://github.com/databricks/spark-redshift/issues/244

Issues with Java version: Spark supports Java 8, not 11 which I had. So I had to install Java-8 and make spark-env.sh point to that

EMR issues:
https://stackoverflow.com/questions/39095655/operation-timed-out-error-on-trying-to-ssh-in-to-the-amazon-emr-spark-cluster
Running in virtualenv on EMR:
Copy the requirements.txt to EMR master Node
Create a virtualenv
Install the dependencies from this requirements.txt file
Run:
/usr/bin/spark-submit --deploy-mode cluster --master yarn --num-executors 2 --executor-cores 8 --executor-memory 8g --packages org.apache.hadoop:hadoop-aws:2.7.0 --conf spark.pyspark.virtualenv.enabled=true --conf spark.pyspark.virtualenv.type=native --conf spark.pyspark.virtualenv.requirements=/home/hadoop/requirements.txt  --conf spark.pyspark.virtualenv.bin.path=/home/hadoop/virtual_env/bin --conf spark.pyspark.python=/home/hadoop/virtual_env/bin/python etl_emr.py

spark-submit --conf spark.pyspark.virtualenv.enabled=true  --conf spark.pyspark.virtualenv.type=native --conf spark.pyspark.virtualenv.requirements=/home/hadoop/requirements.txt --conf spark.pyspark.virtualenv.bin.path=/Users/jzhang/anaconda/bin/virtualenv --conf spark.pyspark.python=/usr/local/bin/python3 spark_virtualenv.py
