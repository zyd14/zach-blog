---
title: Using Spark Connect to Execute Queries on a Databricks Cluster
date: "2024-06-29T22:40:32.169Z"
description: "The promise of Spark Connect is to enable lightweight client applications to submit queries to an existing cluster. Using this pattern can increase resource utilization and enable powerful queries to be executed from lightweight clients. Databricks provides the databricks-connect package for enabling this in development environments, but the overuse of patching it employs is awkward and makes local development difficult, assuming the user wants to run everything on a remote spark cluster. Read more to learn how to use Spark Connect directly to connect to existing spark clusters to enable access to large-scale queries from anywhere... "
---

# Overview of Spark Connect

Spark Connect is a client-server interface for Apache Spark, released with v3.4, that allows for a more flexible and efficient way to interact with Spark clusters. It was introduced to decouple the client application from the Spark cluster, enabling use cases where the client may not have the appropriate hardware to support queries on big data. A few examples of possible use cases are; web applications providing dashboards and querying capabilities, and ETL / ELT pipelines which may need to perform queries on larger data for specific steps, but otherwise don't need a ton of resources. Spark Connect acts as a way for a process / application to access large amounts of compute when it needs it, while sharing that compute with other processes or applications when it doesn't need it.

Let's explore some of the specifics...

## Key Concepts of Spark Connect

1. **Client-Server Architecture:**
   - Spark Connect follows a client-server architecture. The client can run on different environments (such as laptops, cloud services, a Raspberry Pi, etc.) and connect to a Spark cluster remotely.

2. **Decoupling the Client from the Cluster:**
   - Traditionally, Spark applications tightly coupled the driver program with the cluster. Spark Connect separates these components, enabling clients to connect, execute tasks, and retrieve results without being physically located on the cluster.

3. **Multiple Language Support:**
   - Spark Connect aims to provide a unified API across multiple languages, including Python, Scala, and Java. This makes it easier to write Spark applications in the language of choice while benefiting from the same underlying Spark engine. Theoretically any language may implement the API, and some experimental work has already been done with a Rust client. Perhaps someone too smart for their own good will even implement a Javacript Spark Connect client.

4. **Remote Execution:**
   - With Spark Connect, the client can be lightweight and communicate with the Spark cluster over a network. This allows for remote job submission and monitoring, making it easier to manage and scale Spark applications.

## How Spark Connect Works

1. **Client:**
   - The client is a lightweight application that runs on a network-attached device and interacts with the Spark cluster remotely over the network. When an [action](https://spark.apache.org/docs/latest/rdd-programming-guide.html#actions) is triggered in a spark program executing on a Spark Connect client, the query plan is resolved locally in the client, and then submitted synchronously to the remote cluster via the protocol specified in the Spark Connect API. Once the query plan is submitted, the client waits for the results, which are transported over the wire from the cluster to the client.
   - Note: queries that return a large volume of data will be limited by the network bandwidth between the cluster and the client

2. **Spark Connect Server:**
   - The server runs within the Spark cluster and listens for requests from clients. It processes these requests, executes the required tasks on the cluster, and sends the results back to the client.

3. **Execution Flow:**
   - A typical workflow with Spark Connect involves the following steps:
     1. The client establishes a connection to the Spark Connect server.
     2. The client submits a query when an [action](https://spark.apache.org/docs/latest/rdd-programming-guide.html#actions) is triggered in the spark program.
     3. The server receives the request, processes it using the Spark engine, and executes the necessary tasks on the cluster.
     4. The server sends the results back to the client.
     5. The client receives and processes the results.

## Benefits of Spark Connect

1. **Flexibility:**
   - The decoupling of the client and cluster allows for greater flexibility in deploying and managing Spark applications. Clients can run on different environments with different dependencies and connect to the cluster remotely.

2. **Scalability:**
   - Spark Connect enables better scalability as it allows for distributed client applications that can interact with a centralized Spark cluster.

3. **Language Agnostic:**
   - By implementing a language-agnostic communication interface, Spark Connect makes it easier for developers to write Spark applications in their preferred language without worrying about compatibility issues.

4. **Improved Resource Management:**
   - The separation of the client from the cluster can lead to more efficient resource utilization. Clients do not need to consume cluster resources unless they are actively submitting and managing jobs. Many web applications and ETL / ELT jobs spend much of their time doing things other than submitting spark queries. This means that a single cluster could serve many clients, improving the overall utilization of the cluster. Any on-cluster caches (e.g. [Databricks disk cache](https://docs.databricks.com/en/optimizations/disk-cache.html)) will also be able to be reused from session to session as long as the cluster stays on.

5. **Enhanced User Experience:**
   - Users can benefit from a more interactive experience when working with Spark, as the client can provide immediate feedback and results without being constrained by the cluster's resources and latency. This can be helpful for local development where users would like to still be able to test their code on large datasets.

## Limitations and Considerations

1. **Network Overhead:**
   - Spark Connect relies on network communication between the client and the cluster. This can introduce latency and bandwidth constraints, especially when dealing with large volumes of data.

2. **Client Memory Limitations:**
    - The results of each action will be returned to the client, because it is acting as a disconnected driver node. If you're using Spark Connect from clients with limited memory you will need to be cogniscent of this. One solution would be to ensure results are written to object storage instead of collecting them into memory.

3. **Spark UI Monitoring:** #TODO
    - When submitting jobs with the Spark Connect client, the monitoring experience will be a little different than when monitoring jobs submitted to a driver on the cluster. In particular, 


## Example Usage

Here's a simplified example of how a Python client might use Spark Connect to interact with a Spark cluster:

```python
from pyspark.sql.connect import SparkSession

# Initialize the Spark Connect client
spark = SparkSession.builder \
    .appName("SparkConnectExample") \
    .remote("spark://<spark-connect-server>:7077") \
    .getOrCreate()

# Perform some data operations
df = spark.read.csv("s3a://<path-to-data>/data.csv")
df.show()

# Submit a query
result = df.groupBy("column").count()
result.show()

# Stop the client session
spark.stop()
```

## A simple example of connecting to an existing Databricks Cluster

Connecting to a Databricks cluster requires setting the connection string with a few magic parameters. 

TODO: make sure to highlight imports from pyspark.sql.connect

## Why not databricks-connect?

Databricks provides a package called [databricks-connect](https://docs.databricks.com/en/dev-tools/databricks-connect/python/index.html) that facilitates connecting to Databricks clusters using Spark Connect, and works quite well. It is just a thin wrapper around some objects found in `pyspark.sql.connect`. They also provide a VSCode extension that might be worth checking out (I'm a PyCharm guy so I can't comment on how well it works, I hear good things).  However, installing the package introduces a side effect that may be unexpected by the user. 

When you install `databricks-connect` with `pip` it will actually patch over modules within `pyspark.sql` with modules found in `pyspark.sql.connect`. In particular, it will replace the SparkSession class with a custom RemoteSparkSession class fulfilling the same interface, as well as replacing the `pyspark.sql.functions` and `pyspark.sql.types` modules with analogous modules found in `pyspark.sql.connect` that are built to support remote spark sessions. This results in global changes to your code, meaning all pyspark operations now will be run on a remote Databricks cluster. This can make local development difficult, especially when running unit tests that you don't necessarily want to run remotely as that incurs a not-insignificant startup latency (10-30s for the shortest of queries). 

If you're looking to run everything in a given python project on a remote Spark cluster, `databricks-connect` may be a good option. One strategy using `databricks-connect` could be to use a tool like [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv) or [poetry](https://python-poetry.org/) to maintain multiple virtual environments, with and without `databricks-connect`. You could possibly define the `databricks-connect` requirements as an extra dependency group in your pyproject.toml.

However, if you want to connect to an existing Spark cluster without patching the client or generally need more control over the attributes of the connection, Spark Connect may be a better choice. This can simplify writing unit tests that can run locally without having to maintain multiple environments. If you follow best practices around dependency injection (specifically for your spark sessions), then it should be straightforward to enable unit testing your spark code with real transformations while still allowing the production application to leverage a remote spark cluster.
