### Project Description:
Sparkify database has been growing dramastically recently, so the legacy tools and classic relational database couldn't fit to the dataset size. That's why in this project we provide a solution of using Spark as a big data processing tool on top of cloud service (EMR) in order to procecss the raw data of Sparkify. The raw data is stored on the data lake (AWS S3) in a json-like format. An automated ETL pipeline is created to read the raw data from S3 to the EMR service, then transform and model it to a star-schema desgin, and then writing the data gain again to S3 as parquet files.


### Running Python scripts:
The easiest way is to just call the python scripta from the terminal like `python etl.py`. However, in the reality, normally using Docker & Kubernetes to deploy such a pipeline on production, and maybe using Airflow for scheduling the pipeline if needed.

However, in this project, I copied the `etl.py` file to the EMR cluster in order to be able to submit and run a Spark job on the distributed cluster.



