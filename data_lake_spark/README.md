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
