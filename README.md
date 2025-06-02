# 🎵 Real-Time Song Recommendation System using Spark ML

Implementation of a real-time song recommendation system that leverages **Apache Spark ML** for both **collaborative filtering** and **content-based filtering**. The system ingests real-time user interaction data (e.g., song plays, likes) from a web or mobile application and delivers personalized song recommendations.

## 🧠 Features

- Simulation of Real-time user data ingestion via AWS Lambda and Kinesis Data Streams
- Streaming pipeline using Amazon Kinesis Firehose and S3
- Scalable processing using Apache Spark Structured Streaming
- Pre-trained hybrid recommendation model (Collaborative + Content-based filtering) based on various factors like (release date, artist, genre, theme, sound frequencies) using Spark ML
- Output of recommendations stored back into S3
- AWS Glue Crawlers to create Athena-compatible schema
- Query recommendations via Amazon Athena

---

## 📊 Architecture Diagram
![image](https://github.com/user-attachments/assets/4ab1dc8d-166d-4389-ba44-b50b4a1d2004)



## 🚀 How It Works


  - User Interaction (Simulated): When a user plays a song or interacts with the app, the event is sent to AWS Lambda.
  - Stream Ingestion: Lambda sends data to Kinesis Data Streams, which buffers and passes data to Kinesis Firehose.
  - Data Lake Storage: Firehose stores data into Amazon S3 as raw JSON files.
  - Real-time Processing: A Spark job running on EMR or Databricks continuously reads from S3 using Structured Streaming.
  - Recommendation Engine: The Spark job loads a pre-trained hybrid model (ALS + content filtering) and generates top-N song recommendations.
  - Storing Recommendations: The recommended songs per user are written to another S3 bucket or prefix.
  - Cataloguing & Querying: AWS Glue Crawlers scan the S3 data and create Athena tables for querying recommendations in near real-time.


## ⚙️ Requirements
  - Databricks
  - Apache Spark 3.x (Structured Streaming)
  - AWS Resources: Lambda, Kinesis Data Stream, Firehose, S3, Glue, Athena
  - Python 3.8+
  - Dependencies:
      - spark
      - boto3
      - pandas
