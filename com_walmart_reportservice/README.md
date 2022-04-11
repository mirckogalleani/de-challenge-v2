# Extract Hourly Service

## General Description

This service was developed in Scala with Apache Spark and generates the reports requested in the Walmart technical challenge.

## Prerequisities
To run this service, you´ll need the next prerequisites:

- Java 8.
- Maven.

## How to Run
 
### Parameters and Configuration
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


### Execute Test

For do the Test, you must follow the next steps:

- Clone repository

```sh
git clone https://gitlab.falabella.com/data-insights-fcom/data-technology/omniture/com_falabella_extractor_azure.git
cd com_falabella_extractor_azure/
mvn clean test
```


### Run Service

To execute the web service, you must follow the next steps:

- Clone repository.

```sh
git clone https://gitlab.falabella.com/data-insights-fcom/data-technology/omniture/com_falabella_extractor_azure.git
cd com_falabella_extractor_azure/
mvn spring-boot:run
```


If the test are passed you can create the Docker image and upload to Google Cloud Container Registry.

### Run Load Extractor Azure Service

To run this service you must have created a Kubernetes cluster previously, uploaded the configmaps configuration to this service, have uploaded the Docker image of the service in Goocle Cloud Container Registry, have created the yaml job in your local computer, have installed the Google Cloud SDK and have the connection with the respective created Kubernetes Cluster.

To run this service execute the following command:
```sh
kubectl apply -f configmap.yaml
create -f pod.yaml
```

