# Big Data Analytics project at MiNI | Warsaw University of Technology

The project's goal is modeling different cryptocurrency prices based on sentiment analysis of reddit posts, using Big Data solutions.

## Functional Requirements
The user should be able to:
— analyze the trends of cryptocurrency prices,
— analyze the sentiment of Reddit posts for a given cryptocurrency,
— obtain predictions of a cryptocurrency price based on Reddit sentiment analysis, i.e., whether
the price is going to increase or decrease in the near future,
— assess model accuracy on historical data, making price predictions for a past date,
— export the prices of a selected cryptocurrency within a specified time period

## Non-functional Requirements
— The price should be available for the user within half an hour of publishing it in the source
API.
— The dashboard should handle at least 20 concurrent users without noticeable loss of performance.
— Security of shared responsibility model offered by Google Cloud Platform. Google is responsible for network, storage, encryption, and physical security. The team will be responsible
for the security of the data pipeline and the application.
1. Introduction 4
— Scalability – GCP offers a platform for building data ingestion and processing pipelines to
support a wide range of streaming, batch, and near-real-time data sources. To ingest data
reliably, serverless auto-scaling Google Pub/Sub service was used. Additionally, Cloud Dataproc provides a serverless Apache Spark integration for the scalable processing of terabytes
of data.
— Fast recovery without disruption can be achieved by using instance templates for Compute
Engines. Additionally, Cloud Storage was used for backups

## System Architecture - Lambda

The following Lambda architecture elements and their components were used in the project:

1. Batch layer (master dataset storage) – Apache HDFS together with Apache Hive for storing
the raw data. Also, the processed data was stored in Avro format (because updates are
made to rows and there are no columns to be added, it’s more like transactional data – both
Reddit submissions and crypto prices have timestamps).

2. Serving layer – Apache Hive and Cloud BigTable as the NoSQL database has been used to store aggregated
and preprocessed data. It works on top of HDFS of Hadoop project.

3. Speed layer – Google Cloud Pub/Sub and Google Cloud Pub/Sub Lite for queuing real-time messages and Cloud Dataproc
with Apache Spark Structured Streaming for stream processing. For the final delivery, the streaming data was stored in Google BigTable in order to allow presenting the most recent cryptocurrency price to the user and the current sentiment.

![image](https://user-images.githubusercontent.com/56268776/217964835-787a5d06-0910-4044-afdb-f29b017d31f2.png)

## Data Sources

### Streaming Data

- real-time cryptocurrency prices - streaming data API, Coinbase, provides the user with real-time selected cryptocurrency
prices
- Reddit real-time submissions - Reddit real-time streaming data has been collected via HTTP GET requests to the subreddit
URL and PRAW library in Python

### Batch Data
- historical cryptocurrency prices
- historical Reddit submissions 
- training sentiment analysis data
  - Stock-Market Sentiment Dataset
  - Stock Sentiment Analysis Dataset
