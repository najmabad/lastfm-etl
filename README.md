# Top 10 Songs in 50 Longest User Sessions



## Understanding the Input Data
This project utilizes the `Last.fm Dataset - 1K users`, which contains listening habits data for nearly 1,000 users 
from February 2005 up to September 2013. 
This dataset was compiled and distributed by Oscar Celma and is available for non-commercial use.
If you want to download the dataset, you can access it [here](http://ocelma.net/MusicRecommendationDataset/lastfm-1K.html).



## Music Identification Strategy
The input dataset includes MusicBrainz Identifiers (MBIDs) for artists and tracks, which could significantly enhance our music recommendation system.
These identifiers help in accurately distinguishing cases of homonymy (e.g., between songs that share titles but were written by different artists) 
and also between different versions of the same songs (e.g., live vs. studio).

After evaluating the pros and cons, **we decided to integrate MBIDs to uniquely identify songs**, supplementing the combination of artist names and track names.

### Advantages:
- **Precision in Identification**: Including MBIDs improves the precision of our music identification process, 
ensuring that each song is uniquely recognized beyond just titles and artist names. 
- **Enhanced User Experience**: Precisely identified tracks mean that users are recommended the exact version of a song they prefer, 
whether it's a live version, a remix, or a studio recording. 

### Drawbacks:

- **Gaps in Data**: While MBIDs provide a robust method for song identification, they are not universally available across all datasets. 
Our current dataset shows that approximately 11% of tracks lack these identifiers. <br>
To ensure the integrity of our analysis, we exclude entries missing MBIDs.
This approach might risk excluding popular songs just because MBIDs are not consistently recorded.
This suggests that while the use of MBIDs helps us maintain accuracy, the potential for missing key data could vary depending on the source quality and completeness. <br>

However, in our specific case, the most notable impact of considering the MBIDs has been the exclusion of an ambiguous entry, 
  i.e. the "Bonus Track" by The Killers—a track that cannot be definitively linked to a specific song. Hence, it was rightly excluded.



- **Excessive Granularity**: MBIDs distinguish between various recordings of the same song.
For example, Pink Floyd's "Any Colour You Like" appears under several track IDs, 
each representing different versions such as a [studio recording]((https://musicbrainz.org/recording/7c278a16-ae04-460c-88ea-39155cadcd09)) 
or a [live performances](https://musicbrainz.org/recording/20f8d183-6124-4cde-af19-04478f4fdb89).
This level of detail, while beneficial for accuracy, may sometimes introduce complexity that is not valued by all stakeholders. 
In some business contexts, the distinction between different versions of a song may be considered superfluous.

In conclusion, by adopting MBIDs, we aim to balance the need for precise song identification with practical operational requirements.
This strategy will help in minimizing errors in song matching while catering to user preferences for specific song versions, 
thereby enhancing the overall effectiveness of our music recommendation system.




-----



## Workflow and Project Structure Overview

This project is structured to accommodate both exploratory and routine analysis needs.
It uses PySpark within a Jupyter Notebook environment for the former and a more structured ETL framework for the latter.

### Interactive / One-off Analysis
The initial phase of the project involved conducting an exploratory analysis to rapidly understand and visualize the data, as well as to test hypotheses. This type of analysis is typically preferred for one-off requests or when starting a new project to get a feel for the data characteristics and potential insights.

- _Location of Analysis_: All exploratory code and notebooks are located in the `one_off_analysis` folder.
- _How to Run_: See the detailed instructions below.


### Structured ETL Process
After the initial exploratory phase, the project was structured to support regular, repeatable analysis processes.
This structure is particularly useful for analyses that need to be performed on a regular basis, such as weekly reports or automated updates.

- _Location of Code_: All ETL scripts and structured code are contained in the `etl` folder.
- _How to Run_: See the detailed instructions below.



------

## Technology Stack Overview
In this project, we utilize Docker and PySpark, leveraging their capabilities to optimize our development and deployment processes.

### Docker
Docker is integral to our project for several reasons. Primarily, it's used to simulate Spark cluster environments during development and testing. 
We configure Docker to create two separate Spark clusters:

- **Application Cluster**: One cluster is dedicated to running the application. This setup allows us to simulate how the application will perform in a production-like environment.
- **Testing Cluster**: A separate cluster is used exclusively for running automated tests. This isolation helps to ensure that testing does not interfere with the ongoing development or production environments, maintaining the integrity and reliability of our testing procedures.

In production, the application would connect with a real Spark cluster, taking advantage of genuine distributed computing resources to manage and process large datasets effectively.

### PySpark

PySpark was selected for its robust data processing capabilities, especially its proficiency in handling large-scale 
data operations and complex transformations within a distributed computing environment. <br>
Although the current application processes approximately 2.5 GB of data—which does not constitute a high volume by modern standards—PySpark equips us with the necessary infrastructure to scale up and handle larger datasets as needed.


----

## One-off Analysis

### Requirements
- Docker
- Docker Images Details:
  - Image Name: `quay.io/jupyter/pyspark-notebook:latest`
  - Size: 4.69 GB

### How to Run It
To perform one-off analyses using the provided Makefile commands, follow these simplified steps:

1. **Ensure Docker and Docker Compose are Installed**:
   - Verify that Docker and Docker Compose are installed on your machine. If not, install them from [Docker's official site](https://docs.docker.com/engine/install/) and Docker Compose installation guide.


2. **Start the PySpark Notebook**:
   - Open a terminal window and navigate to the `one_off_analysis` folder
     ```bash
     cd lastfm/one_off_analysis
     ```
     Once in the directory, execute:
     ```bash
       make pyspark-notebook-start
     ```
     This command will build the Docker images if they aren't already built, and start the necessary containers in detached mode.
   

3. **Access the Notebook**:
   - After the containers are up, the Jupyter PySpark notebook should be accessible by navigating
   to http://localhost:8888 in your web browser. <br> 
   _Troubleshooting_: If port 8888 is already in use, change the mapping in the `compose.yaml` file, e.g. `"8889:8888"`,
   stop and start the container again, and then go to http://localhost:8889

   - Run the notebook and execute the analysis.
   - To view the Spark Jobs, the data stored in cache, the execution plan, or other information go to http://localhost:4040 


4. **Stop the Notebook**:
    - When you are done with your analysis and wish to stop the notebook, execute:
      ```bash
      make pyspark-notebook-stop
      ```
      This command will stop the containers and network created for the PySpark notebook.
 

5. **See the Results**:
   - The top 10 songs played in the 50 longest sessions are available in a tab separated text file under the `results` folder.



6. **Teardown the Environment**:
    - If you wish to completely remove all containers, networks, and images related to the project, execute:
      ```bash
        make pyspark-notebook-teardown
      ```

      This command cleans up everything, ensuring no residual data or images are left on your system, 
      freeing up space.



----

## ETL
### Requirements
- Docker
- Docker Images Details:
  - Image Name: `lastfm/etl/pyspark/analysis`
  - Size:  1.93 GB
  - Image Name: `lastfm/etl/pyspark/tests`
  - Size:  1.96 GB

### ETL Process Overview
The ETL component of this project is designed to handle the extraction, transformation, and loading of data efficiently, 
leveraging Apache Spark to manage large datasets. <br>
The entry point for the ETL process is the `top_songs.py` script.
This script orchestrates the entire ETL workflow as described below:

#### Workflow
1. **Configuration Loading**: The script starts by loading configuration to easily inject development/production settings.
2. **Data Extraction**: Data is extracted from url source and save in the `data`.
3. **Transformation**: The extracted data undergoes transformation to enhance the music events dataframe with session information
and to create a session metrics dataframe (i.e. the sessions and their duration).
4. **Analysis**: The two transformed dataframes are used to analyse the top songs in the longest session.
5. **Data Loading**: Finally, the transformed data is saved as a TSV file in the `results` directory.

### How to Run It

To run the ETL using the provided Makefile commands, follow these simplified steps:

1. **Ensure Docker and Docker Compose are Installed**:
   - Verify that Docker and Docker Compose are installed on your machine. If not, install them from [Docker's official site](https://docs.docker.com/engine/install/) and Docker Compose installation guide.

2. **Submit the Spark Application**:
   - Open a terminal window and navigate to the `etl` folder
     ```bash
     cd lastfm/etl
     ```
     Then run:
     ```bash
     make deploy-app
     ```
     This will:
     - Start a Spark cluster on Docker (one master, one worker)
     - Run the `top_songs.py` Python application on the Spark standalone cluster
     - Stop the Spark cluster
     
   - Alternatively, you can run the three actions separately:
     - Start the cluster:
       ```bash
       make spark-cluster-start
       ```  
     - Submit the application:
       ```bash
       make submit-top-songs-app
       ```
     - Stop the cluster:
       ```bash
       make spark-cluster-stop
       ```

   - To view the Spark Jobs, the data stored in cache, the execution plan, or other information go to http://localhost:4041


3. **See the Results**:
   - The top songs played in the longest sessions are available in a tab separated text file under the `results` folder.


4. **[Optional] Run the Test Suite**:
   - Run the test suite:
     ```bash
     make test-run
     ```


5. **Teardown the Environment**:
    - If you wish to completely remove all containers, networks, and images related to the project, execute:
      ```bash
      make teardown-all
      ```

      This command cleans up everything, ensuring no residual data or images are left on your system, 
      freeing up space.



### [Optional] Customizing Application Execution
The `submit-top-songs-app` command in the Makefile supports passing arguments to customize various parameters of the Spark application. 
This flexibility allows you to tailor the application's execution to your specific needs, such as adjusting the session threshold, 
the number of sessions or songs analyzed, and selecting configuration settings suitable for different environments like development or production.

#### Using Command-Line Arguments
You can pass these parameters directly when running the make command to override the default settings specified in the Makefile. 
Here's how you can customize each parameter:

- **Configuration File**: Specify a different configuration file to load settings appropriate for different environments (e.g., production, development).
  ```bash
  make submit-top-songs-app CONFIG=config/prod.json
  ```

- **Session Threshold**: Set a custom session threshold to define the minimum duration (in minutes) that separates different listening sessions.
  ```bash
  make submit-top-songs-app SESSION_THRESHOLD=30
  ```
  
- **Number of Sessions**: Change the number of longest sessions to analyze.
  ```bash
  make submit-top-songs-app SESSION_NUMBER=100
  ```
  
- **Number of Songs**: Adjust the number of top songs to retrieve from the analyzed sessions.
  ```bash
  make submit-top-songs-app SONG_NUMBER=50
  ```

#### Example Usage
Here is an example of how to run the application with a specific configuration for a production environment, 
a higher session threshold, and an increased number of sessions and songs:

```bash
make submit-top-songs-app CONFIG=config/prod.json SESSION_THRESHOLD=25 SESSION_NUMBER=100 SONG_NUMBER=20
```


## Improvements

### Incremental Load + Partitions
To optimize data processing in production, transition to an incremental load approach, focusing only on ingesting 
and analyzing new or changed data. <br>
Moreover, ensure that the data is partitioned in an efficient way.

### Scheduling 
Automate the ETL workflows (e.g. using Apache Airflow) to manage and schedule tasks efficiently. <br>
In Airflow, we could employ the PythonOperator for data extraction and SparkSubmitOperator for data transformations and analytics.

### Data Quality
Add a library to check for data quality in the input data source.
For example, we could use GreatExpectation and set the following expectations:
```python

events.expect_column_values_to_not_be_null('timestamp')
events.expect_column_values_to_not_be_null('musicbrainz_track_id', mostly=0.85)
events.expect_column_values_to_not_be_null('track_name', mostly=0.99)
events.expect_column_values_to_not_be_null('artist_name', mostly=0.99)

```

This would work best if the events are ingested incrementally, and we 
validate the new data only.

### Intermediate Storage + Final Storage
Implement intermediate data storage strategies to save checkpoints during processing. 
This allows for quick recovery from any process interruptions, enhancing system resilience and efficiency.

In this project, we employ `cache()` to hold the enhanced events with sessions in memory, which helps prevent redundant 
CSV reads. <br>
For production environments, especially when dealing with data volumes too large for memory caching, 
consider implementing more scalable caching strategies or using disk-based persistence options.

Regarding the storage of the end results, we currently mound the Docker folder to a host machine.
This is not something we would do in production.

### DevOps
For production environments, it's crucial to ensure that the Spark cluster is configured properly on a reliable 
cloud platform such as AWS, Google Cloud, or Azure. 
This setup will allow the Spark cluster to efficiently scale according to the computational demands and data volumes, 
ensuring optimal performance.

If the application is containerized using Docker, managing these containers with Kubernetes offers robust orchestration capabilities. 

### Security / Data Policies
Examples:
- Create a service account on the cloud platform to run the application.
- Ensure data is encrypted (if needed).
- Ensure data is stored in the correct location.
- Load config settings in a more robust way.

-------

### References

- [Last.fm](https://www.last.fm/). Last.fm Dataset - 1K users (2010). Retrieved from [Music Recommendation Datasets for Research](http://ocelma.net/MusicRecommendationDataset/lastfm-1K.html)

