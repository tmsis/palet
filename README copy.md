
# spark

A `debian:stretch` based [Spark](http://spark.apache.org) container. Use it in a standalone cluster with the accompanying `docker-compose.yml`, or as a base for more complex recipes.

## crealytics spark-excel
The `spark-excel` package was configured to be included when the cluster is started using Docker compose. When referring to the Git documentation, note the latest version listed in the usage section is not correct. Be sure to cross reference to Maven in order to identify the correct version.

## docker example

To run `SparkPi`, run the image with Docker:

    docker run --rm -it -p 4040:4040 docker.io/library/spark-base:3.0.2 bin/run-example SparkPi 10

To start `spark-shell` with your AWS credentials:

    docker run --rm -it -e "AWS_ACCESS_KEY_ID=YOURKEY" -e "AWS_SECRET_ACCESS_KEY=YOURSECRET" -p 4040:4040 docker.io/library/spark-base:3.0.2 bin/spark-shell

To do a thing with Pyspark

    echo -e "import pyspark\n\nprint(pyspark.SparkContext().parallelize(range(0, 10)).count())" > count.py
    docker run --rm -it -p 4040:4040 -v $(pwd)/count.py:/count.py docker.io/library/spark-base:3.0.2 bin/spark-submit /count.py

## docker-compose example

To create a simplistic standalone cluster with [docker-compose](http://docs.docker.com/compose):

    docker build --no-cache -t docker.io/library/spark-base:3.0.2 .\services\spark\.
    docker-compose -f .\services\spark\docker-compose.yml up

The SparkUI will be running at `http://${YOUR_DOCKER_HOST}:8080` with one worker listed. To run `pyspark`, exec into a container:

    docker exec -it docker-spark_master_1 /bin/bash
    bin/pyspark

To run `SparkPi`, exec into a container:

    docker exec -it docker-spark_master_1 /bin/bash
    bin/run-example SparkPi 10

# Databricks
Be sure to configure Databricks CLI access with tokens (and use the --profile flag to differentiate CMS instances from ours).

## Configure the CLI
databricks configure --token --profile <profile-name>

After you complete the prompts, your access credentials are stored in the file ~/.databrickscfg on Unix, Linux, or macOS, or %USERPROFILE%\.databrickscfg on Windows. Modify this file to add the insecure option and set its value to "True" in order to connect to CMS Databricks instances.

Example profiles:

    [dev]
    host = https://databricks-dev-data.macbisdw.cmscloud.local
    token = <secret>
    insecure = True

    [val]
    host = https://databricks-val-data.macbisdw.cmscloud.local
    token = <secret>
    insecure = True

    [prod]
    host = https://databricks-prod-data.macbisdw.cmscloud.local
    token = <secret>
    insecure = True

## Build a Wheel
    rm -r -fo .\build; rm -r -fo .\*.egg-info
    python setup.py bdist_wheel
    databricks --profile val fs cp ./dist/palet-1.0.0-py3-none-any.whl dbfs:/FileStore/shared_uploads/akira/lib/palet-1.0.0-py3-none-any.whl --overwrite

After running the above commands, go to the Web UI > Clusters > Libraries > Install New...
Select DBFS/S3 and provide the location where the WHL file was uploaded above. Then, Select the Library Type of "Python Whl," paste the file path and click install.

Note after installing, you may need to detach and reattach any notebooks several times before being able to use the library.

## Stack CLI
The stack CLI provides a way to manage a stack of Databricks resources, such as jobs, notebooks, and DBFS files. You can store notebooks and DBFS files locally and create a stack configuration JSON template that defines mappings from your local files to paths in your Databricks workspace, along with configurations of jobs that run the notebooks.

Use the stack CLI with the stack configuration JSON template to deploy and manage your stack.

To deploy a stack to a workspace:

    databricks --profile dev stack deploy .\config.json

If a job was defined, it can be run after deployment (the job ID is echoed during deployment and saved to config.deployed.json):

    cat ./config.deployed.json
    databricks --profile dev jobs run-now --job-id <job ID>

## To remove created items

    databricks --profile dev fs rm <dbfs:/ path>
    databricks --profile dev workspace delete "<path>" --recursive
