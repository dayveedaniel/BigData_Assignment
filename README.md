# Project Documentation

## Introduction
This repository contains all the necessary files, scripts, and instructions to successfully set up and run the project. Follow the steps below to ensure your environment is correctly configured and data is processed effectively.

## Repository Structure
```
/
├── /cleaned_data            # procesed dataset
├── /data                    # initial dataset 
├── /db_models               # contains .hck.json file from hackolade
├── /scripts                 # All script files (SQL, MongoDB, Neo4J)
├── /screenshots             # Screenshots of results and processes
├── /output                  # Results of experiments
├── docker-compose.yml       # Docker setup file
├── .gitignore
├── report.pdf               # Final project report
├──  README.md               # Project documentation
└── requirements.txt         # required python packages for this project
```

## Setup Instructions

### Step 1: Clone the Repository
```bash
git clone <repository-link>
cd <repository-name>
```

### Step 2: Create a virtual python environment and install required python packages
```bash
pip install -r requirements.txt
```

### Step 3: Docker Configuration
To set up all the necessary containers:
```bash
docker-compose up
```
This command pulls the latest images of (Postgres, MongoDB, Noe4j and OrioelDB) from Docker Hub, mounts the project directory into the required directories for each container, and sets environments and ports.

### Step 4: Data Preparation
1. Create a `/data` folder in the root directory.
2. Load the required CSV files into the `/data` folder. This folder is added to `.gitignore` to prevent large files from being uploaded to GitHub.

### Step 5: Data Loading
#### PostgreSQL
Ensure you have a folder named `/project` in the root directory. This is automatically mounted to `/project` in the PostgreSQL container via the Dockerfile.

To load data into PostgreSQL:
```bash
sudo psql -d ecommerce -f /project/scripts/load_data_psql.sql
```

#### MongoDB
To load data into MongoDB:
1. Access the MongoDB container:
```bash
docker exec -it mongodb bash
```
2. Run the following command to execute the MongoDB script:
```bash
mongosh --file project/scripts/load_data_mongodb.js
```

The `clean_data.py` script processes MongoDB files into `.json` format for easier importing. Large files are split to avoid system write limits.

#### Neo4J
The `/import` folder in the Neo4J container is mounted to simplify file imports. To load data into Neo4J:
1. Access the Neo4J container:
```bash
docker exec -it neo4j bash
```
2. Run the following command to import data:
```bash
cypher-shell -u neo4j -p 12345678 -f /import/scripts/load_data_neo4j.cypher
```

If you encounter a `Java heap space Exception`, increase the memory settings in the Neo4J configuration file (`neo4j_data/conf/neo4j.conf`) if you mounted the volume as in the docker compose file or cd into `neo4j/conf/neo4j.conf` and use a command line editor to add the following parameters:
```bash
server.memory.heap.initial_size=3g
server.memory.heap.max_size=4g
server.memory.pagecache.size=2g
db.memory.transaction.total.max=4g
db.memory.transaction.max=4g
dbms.memory.transaction.total.max=4g
```

Increase the heap max size to **10GB** to ensure the `load_data.cypher` script runs successfully.

Alternatively, you can run the Python script `load_data_neo4j.py` to load data in batches via the Neo4J Python driver.

#### OrioleDB
This is an extension to postgres to make it faster, it uses a different storage engine. Things to take not then using OrioleDB extension in postgres. OrioleDB tables support only ICU, C, and POSIX collations. So, make sure the cluster or database is set up with default collations that fall under those options, otherwise you have to write COLLATE for every "text" field of the table.
Therefore when creating a database you should specify `LOCALE_PROVIDER` as so:
```bash
CREATE DATABASE test
LOCALE_PROVIDER icu
ICU_LOCALE "en-US"
LOCALE "en_US.utf8"
TEMPLATE template0;
```


## Important Notes
- Ensure that your `.gitignore` file includes the `/data` folder and other large files that should not be uploaded.
- Follow the provided instructions carefully to avoid errors during data processing.
- Neo4j database wasn't benchmarkes because after so many trials, I didn't have enough recources to upload data from huge csv files to the database even after splitting to smaller chunks because it requires a lot of RAM about (10GB).
- From my results using OrioleDB extension on postgres didn't make the queries faster , but even significantly slowed down the query times(results in `Big_data_report.pdf`). Most likely it's due to an issue from my usage of this extension.


