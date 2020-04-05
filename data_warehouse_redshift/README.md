## Project description:
In this project, we build a DW on cloud using AWS Redshift. We model our data as a star-schema dimensional model. The data consists of listening event logs from a music app Sparkify and data about songs, artists, and users.
The process looks like the following:
1. First we stage the logs from S3 to staging tables in Redshift
2. We design our DW schema with appropriate fact and dimension tables, and come up with good sortkey and distkey
3. Next, we transform data from our staging table and load it into our star-schema tables to support analytical queries

## Setup instructions:
### Step 1: Create AWS account:
We can do so by following this link: https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/
### Step 2: Create an IAM user:
- Create an IAM user and give it `AdministratorAccess`.
- Take note of the access key and secret
- Save the access key and secret in a dwh.cfg file under:
```
[AWS]
KEY= YOUR_AWS_KEY
SECRET= YOUR_AWS_SECRET
```

### Step 3: Create IAM role:
- Using boto3 API, create a new IAM role to allow redshift cluster to call other AWS services (you can load above saved AWS key and secret to access AWS resources via boto3)
- Attach `AmazonS3ReadOnlyAccess` policy to this IAM_ROLE
- Get the IAM_ROLE ARN
- Save the ARN under:
```
[IAM_ROLE]
ARN=
```

### Step 4: Create a redshift cluster
- Use `dc2.large` instance type, `multi-node` cluster type, and select `4` nodes
- We can either do this manually in AWS console or by using boto3 API
- Using boto3 API is a really good practice and helps us do `infrastructure-as-code`
- Let the cluster get started and get the cluster's host address: `<your-cluster-idenfitier>.<some-domain>.<region>.redshift.amazonaws.com`

### Step 4: Prepare dwh.cfg file
This file is used in both the scripts: create_tables.py and etl.py
```
[CLUSTER]
HOST=<cluster's host address obtained in step 3>
DB_NAME=<name-of-db>
DB_USER=<db-user>
DB_PASSWORD=<password>
DB_PORT=5439

[IAM_ROLE]
ARN=<ARN obtained in step 2>

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'
```

### Step 5: Run ETL
1. Run create_tables.py from data_warehouse_redshift directory (this will drop and create tables needed for this project)
2. Run etl.py which will perform steps 1-3 as described in project-description

### Step 6: Perform analytical queries
