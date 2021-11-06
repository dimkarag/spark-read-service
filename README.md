# Spark Read Service
Spark Read Service is a Spring Boot Application which uses Apache Spark in order to read tables of a database and return the row data in a Json string and optional store it to parquet files.
You can read data using the api provided.

## Table of Contents
- [Install](#install)
- [API](#api)
- [License](#license)
## Install & Run
- Java 11 required.
- Apache Spark v3.0.0 required
To install & run locally (for development and testing):
```
1) git clone https://github.com/dimkarag/spark-read-service.git
2) cd spark-read-service/src/main/resources
3) copy application.yaml.dist and create an application.yaml file.
4) edit application.yaml file setting the desired configuration 
   for database and apache spark.
5) run a Spark Master node and a Worker node.
6) run SparkSqoopTablesApplication main class.

```
## API

## License
[MIT](https://choosealicense.com/licenses/mit/)
