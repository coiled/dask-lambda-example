
# What is this?

Example AWS Stack written in Python using [CDK](https://docs.aws.amazon.com/cdk/v2/guide/home.html), to
demonstrate using AWS Lambda as a Dask client to coordinate and offload work to an existing cluster.

---

# What is in the stack?

The core example is files landing in S3, which then trigger a Lambda function. This function connects to an
existing Dask cluster to submit the file for processing.

To complete the example, the stack deploys the S3 bucket along with an example 'producer' to mimick a data 
vendor which deposits files every minute. From there the example compute uses the input from the file to do
some arbitrary computations on the cluster.

---

# How to try?

## Build

```bash
$ python -m venv .env
$ source .env/bin/activate
$ pip install -r requirements-dev.txt
$ pip install -r requirements.txt
```

This will install the necessary CDK, then this example's dependencies, and then build your Python files and your CloudFormation template.

Install the latest version of the AWS CDK CLI:

```shell
$ npm i -g aws-cdk
```

### Run

```bash
cdk deploy \
  --toolkit-stack-name <<bootstrap CDK stack name, if not the default>> \
  --parameters CoiledToken=<<your coiled token>> \
  --parameters CoiledAccount=<<your coiled account name>> \
  --parameters CoiledUser=<<your coiled user>>
```

From there, you can go to Lambda functions run the `StartStop` Lambda function 
with input of `{"action": "start"}`, (or wait for the scheduled CRON event). Then
go to `SecretsManager` or your Coiled account to view the new cluster and should see
new computations taking place every minute. 

You can also manually initiate new files by going to the `Producer` Lambda function and
running it (input doesn't matter).
