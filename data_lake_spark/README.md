Steps I went through to do this project:
1. Get a sense of the data locally by running pyspark shell locally or setup jupyter locally
2. Run same code on EMR notebook with S3 paths and see that the code can handle large dataset efficiently
3. Modify the script to reflect the tested notebook code and run it on EMR cluster

Steps to run the code:
On local:
1. Setup Spark using this [guide](https://datawookie.netlify.app/blog/2017/07/installing-spark-on-ubuntu/)
2. Create Jupyter kernel for PySpark using this [guide](https://anchormen.nl/blog/big-data-services/pyspark-jupyter-kernels/)
Or just create a kernel using this JSON file: `kernel.json` in your jupyter kernels directory
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
3. Make your PYSPARK_PYTHON variable point to your virtual_env Python so that it uses the right python version
4. Now in the virtual_env, run `jupyter notebook` and you should have a PySpark kernel with `spark` variable containing the `SparkSession` initialized

Running etl.py as a local script:
1. Run this from virtual_env and data_lake_spark directory: `$SPARK_HOME/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:2.7.0 etl.py`
2. Note: you might have to set this spark option:
```
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.access.key", access_id)
hadoop_conf.set("fs.s3a.secret.key", access_key)
```
Run on EMR:
I did not run in EMR notebook because I ran the code on local notebook and was confident that it works efficiently but if you're not then spin up EMR cluster with Spark 2.4 and run the `data_exploration.ipynb` there with modified paths to read S3 instead of local
1. SSH to master node
2. Copy requirements.txt
3. Create virtual_env with this requirements.txt file
4. Run `/usr/bin/spark-submit etl_emr.py`

Next steps:
1. Use bootstrap script to setup the environment and submit step on EMR using add-step

<h1>
</h1>

Notes to self:
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
Run [THIS DID NOT WORK]:
```/usr/bin/spark-submit --deploy-mode cluster --master yarn --num-executors 2 --executor-cores 8 --executor-memory 8g --packages org.apache.hadoop:hadoop-aws:2.7.0 --conf spark.pyspark.virtualenv.enabled=true --conf spark.pyspark.virtualenv.type=native --conf spark.pyspark.virtualenv.requirements=/home/hadoop/requirements.txt  --conf spark.pyspark.virtualenv.bin.path=/home/hadoop/virtual_env/bin --conf spark.pyspark.python=/home/hadoop/virtual_env/bin/python etl_emr.py```
Another idea is to use bootstrap actions:
https://towardsdatascience.com/production-data-processing-with-apache-spark-96a58dfd3fe7
Put both the bootstrap script and etl script on S3.
Use aws emr create-cluster and add-actions to run the ETL

Installing packages/jars and using them in Pyspark Jupyter notebook setup:
https://stackoverflow.com/questions/35762459/add-jar-to-standalone-pyspark/50142102
See how I have modified the kernel.json's PYSPARK_SUBMIT_ARGS argument