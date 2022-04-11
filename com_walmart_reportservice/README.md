# Report Service

## General Description

This service was developed in Scala with Apache Spark and generates the reports requested in the Walmart technical challenge.

## Prerequisities
To run this service, you´ll need the next prerequisites:

- Java 8.
- Maven.

## How to Run
 
## Parameters and Configuration
Environment Variables:

Contract:
The contract must be encoded in base 64. And the format to the contract is a json with this model:
| Field name | Type | Description |
| --- | --- | --- |
| `input` | String | This variable represent the where the input data was stored.
| `output` | String | This variable represent where the output data is going to be stored.
| `inputFileFormat` | String | This variable represent the file format for the input data. This variable can take the following values: csv, avro and json.
| `outputFileFormat` | String | This variable represent the file format for the output data. This variable can take the following values: csv, avro and json.
| `sparkMode` | String | This variable represent the Spark mode in which is going to be submitted the spark job. This variable can take the following values: cluster and local.

eg:

```json
{
	"input": "/opt/app/input/*.json",
	"output": "/opt/app/output/",
	"inputFileFormat": "json",
	"outputFileFormat": "csv",
	"sparkMode": "local"
}
```


## Execute Test

For do the Test, you must follow the next steps:

- Clone repository
```sh
git clone https://github.com/mirckogalleani/de-challenge-v2.git
```

- Navigate the service folder and run the tests
```sh
cd de-challenge-v2/com_walmart_reportservice/
mvn clean test
```

## Run Report Service Locally

To run this service you must have installed Java 8 in  your local computer.

To run this service you must be located in the root folder of this repository and execute the following commands:

```sh
git clone https://github.com/mirckogalleani/de-challenge-v2.git \
cd de-challenge-v2 \
wget https://www.apache.org/dyn/closer.lua/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz \
cd com_walmart_reportservice \
mvn clean package -DskipTests \
path_where_spark_was_downloaded/bin/spark-submit --class com.walmart.App path_where_the_service_jar_was_created/job.jar base64encodedContract
```

eg:
```sh
git clone https://github.com/mirckogalleani/de-challenge-v2.git \
cd de-challenge-v2 \
wget https://www.apache.org/dyn/closer.lua/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz \
cd com_walmart_reportservice \
mvn clean package -DskipTests \
/opt/app/spark/bin/spark-submit --class /opt/application/job.jar eyJpbnB1dCI6ICIvb3B0L2FwcC9pbnB1dC8qLmpzb24iLCAib3V0cHV0IjogIi9vcHQvYXBwL291dHB1dC8iLCAiaW5wdXRGaWxlRm9ybWF0IjogImpzb24iLCAib3V0cHV0RmlsZUZvcm1hdCI6ICJjc3YiLCAic3BhcmtNb2RlIjogImxvY2FsIn0K
```

## Run Report Service Locally with Docker

To run this service you must have installed Docker in your local computer.

To run this service you must be located in the root folder of this repository and execute the following commands:
```sh
docker build -t image-id:image-version . \
docker run -e CONTRACT="base64encodedContract" -it image-id:image-version
```
eg:
```sh
docker build -t report-service:1.0 . \
docker run -e CONTRACT="eyJpbnB1dCI6ICIvb3B0L2FwcC9pbnB1dC8qLmpzb24iLCAib3V0cHV0IjogIi9vcHQvYXBwL291dHB1dC8iLCAiaW5wdXRGaWxlRm9ybWF0IjogImpzb24iLCAib3V0cHV0RmlsZUZvcm1hdCI6ICJjc3YiLCAic3BhcmtNb2RlIjogImxvY2FsIn0K" -it report-service:1.0
```

## Run Report Service in GCP

To run this service you must have created a Cloud Dataproc cluster with Spark version 3.2.1 previously and have installed Google Cloud SDK in your local computer.

To run this service execute the following command:
```sh
gcloud dataproc jobs submit spark --cluster=cluster-id --region=region --class=com.walmart.App --jars=gs://path/service.jar -- base64encodedContract
```


eg:
```sh
gcloud dataproc jobs submit spark --cluster=report-test --region=us-central1 --class=com.walmart.App --jars=gs://mrgalleani-231fga2-853b-de43-1232-15b86c9fcb8a/report/jar/report-0.0.1-SNAPSHOT-shaded.jar -- eyJpbnB1dCI6ICIvaG9tZS9tcmdhbGxlYW5pL0RldmVsb3BtZW50L3dhbG1hcnQvd2FsbWFydC9kYXRhL2lucHV0LyouanNvbiIsICJvdXRwdXQiOiAiL2hvbWUvbXJnYWxsZWFuaS9EZXZlbG9wbWVudC93YWxtYXJ0L3dhbG1hcnQvZGF0YS9vdXRwdXQvIiwgImlucHV0RmlsZUZvcm1hdCI6ICJqc29uIiwgIm91dHB1dEZpbGVGb3JtYXQiOiAiY3N2IiwgInNwYXJrTW9kZSI6ICJjbHVzdGVyIn0=
```

**Note**
It you want to run this service in a cluster you must modify the contract to cluster mode in the sparkMode variable.