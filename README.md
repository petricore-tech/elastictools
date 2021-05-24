# ElasticTools - high-speed utilities for ElasticSearch data manipulations

`ElasticTools` is a python package developed as a handy utility for cases when it comes to **streaming**, **manipulation** or **processing** of any sort of data stored (or prone to be stored) within ElasticSearch.

The key features are:

- **Highly performant**. Written purely in python with a dense usage of concurrency thanks to AsyncIO.

- **Intuitive**. Though almost every commandline argument has a detailed explanation of *what* is it and *why* we need it, one can intuitively answer those questions.

## Installation

### Create and activate a virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### Install the package using **pip**

```bash
pip install elastic-tools
```

## Usage

### **1. Dump Elasticsearch index**

```bash
elasticdump --elastic_address <ES_ADDRESS> --index <INDEX_NAME> --output_dir <TARGET_DIRECTORY> --chunk_size <CHUNK_SIZE>
```

*Example call:*

```bash
elasticdump --elastic_address localhost:9200 --index products --output_dir ./dump/ --chunk_size 500
```

### **2. Restore Elasticsearch index**

```bash
elasticload --elastic_address <ES_ADDRESS> --index <INDEX_NAME> --input_dir <DIR_WITH_DUMPED_DATA> --chunk_size <CHUNK_SIZE> --connection_pool_size <PARALLEL_CONNECTIONS_COUNT> --mode <MODE>
```

*Example call:*

```bash
elasticload --elastic_address localhost:9200 --index products --input_dir ./dump/ --chunk_size 1000
```

Based on the situation you may want to load *only data* or *both data and settings*. For this purposes `mode` argument is very helpful. Run `mongo2elastic -h` for more info.

### **3. Stream data from MongoDB to Elasticsearch**

```bash
mongo2elastic --mongo_address <MONGO_ADDRESS> --mongo_db <MONGO_DATABASE_NAME> --mongo_collection <MONGO_COLLECTION_NAME> --elastic_address <ES_ADDRESS> --elastic_index <ES_INDEX_NAME> --batch_size <BATCH_SIZE> --connection_pool_size <CONNECTION_POOL_SIZE> --mode <MODE>
```

*Example call:*

```bash
mongo2elastic --mongo_address localhost:27017 --mongo_db store --mongo_collection products --elastic_address localhost:9200 --elastic_index products --batch_size 500 --connection_pool_size 5 --mode default
```

Based on the situation you may want to stream *only data*, *only updates* or *both*. For this purposes `mode` argument is very helpful. Run `mongo2elastic -h` for more info.
