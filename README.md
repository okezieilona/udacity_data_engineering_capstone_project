# Project summary
In a bid to grow the united states tourism industry, the united stated has decieded it is valuable to make the united state tourism data availble to the public to aid travel agencies and other institutions provide better, more effective tourism services to prospective tourist.
Examples:
1. Companies in the hospitality business should be able to analyse trends such as which US cities are visited the most by specific nationals. What time of the year they visit, and if there is a corellation between number of tourist and the racial population that reside in those cities etc. Insights gotten could help them proactively plan for prefrences of their would be customers.

2. Travel agencies should be able to offer advisroy services to their clients. Such advisory service may include
    a. Demographics of cities in the US they intend to visit
    b. The average temperature in those cities at different months of the year
    C. Available airports and airport types in those cities or state.

As the data engineer selected to implement this initiative, I have created a data repository on AWS using a star data schema for the following reasons.
1. AWS was used because of the ease and speed with which it can be scaled as data and users grow.
2. Minimal infrastructure and staff cost (only used resource is paid for)
3. Leverage AWS security features
4. The data was cleaned and stored in AWS s3 buckets in a star data model schema to enable instiutions easily read the data as is into their OLAP enviroment without having to do any cleaning or transformation.



# Python script execution

To create the needed data repository, follow the steps below:

Step 1: Update and save the di.cfg (config) file with the appropriate credentials if required 
Step 2: a. Read airport, city, immigration, temperature data using spark. Also obtain transport, country, and visa data from the label description provided.
        b. format data columns for the respective tables, pivot where neccessary to eliminate multiple rows for a single item in a table, filter out invalid rows.
        c. enrich tables with data gotten from other tables/labels.
        d. write processed data to AWS S3 buckets.
        e. runs data quality scripts that compares data count from source to data in the S3 to check for accuracy. 
        f. Also check that there are no duplicate rows in the  dimension tables.
        All stages of step 2 is achieved using ETL by running "python3 capstone_etl.py"


# Repository files
The list below are the files in the repository
**dl.cfg**: This is a config file that should contain AWS connection parameters.
**etl.py**: This is a Python script that extrcats transforms and loads data into the respective dimensional and facts tables on AWS S3 bucket.
**sql_queries.py**: Python file that contains all sql scripts for spark sql temp tables
**data model.pdf**: This contains a conceptual representation of the data model.
**data dictionary**: This contains description of all columns in each table.


# Approach that can be taken under the following scenarios:
1. The data was increased by 100x: Same approach that was taken for this project will be used. however different data partitions might be considered. Also AWS EMR that allows Spark to be scaled to process data using multiple clusters can be used to avoid the bottleneck of processing the data on a single machine.

2. The data populates a dashboard that must be updated on a daily basis by 7am every day: As opposed to writing the data to aws s3 buckets, the dimension and facts
   tables would be written to a Redshift Postgres DB.
   Airflow can be used for data orchestration where each table load and data quality check is done by a seperate dag. And scheduled to run at 7am daily.

3. The database needed to be accessed by 100+ people: Here, a database that is optimised to handly concurrent connection will be appropriate. Instead of writing the data to s3 as done in this project, The tables can be writen to a Redshift cluster. Redshift can handle up to 500 concurrent connections. Workload Managemt and concurency scalling can be used to optimize performance if there are hundreds of users running queries at the same time.

ppp


