I utilized the Lavenshtein algorithm to identify matches between the US sanction list and UK sanction list due to the absence of a common key. For entities in the datasets, I solely considered the name column to compute the Lavenshtein percentage between the US and UK datasets. For individuals, I considered three columns, namely name, aliases, and Date of Birth. I calculated the Lavenshtein match percentage for all three columns individually, and also computed a column labeled weighted_similarity_percentage by assigning weights to name, alias, and DOB, respectively. I assigned a weight of 85 to name, 10 to DOB, and 5 to alias. These weights can be adjusted and additional columns, such as city, country, etc., can be considered.

This code is not an optimized version as its primary objective is to demonstrate the various stages of the process. Most of the table operations are performed using queries, and the tables will be written to Hive at various stages. To examine the data at different stages, I created multiple tables in Hive.

Below, I have provided the spark-submit command used to run the application.


STEPS TO RUN : 

1) Place the ofac.jsonl and gbr.jsonl in HDFS
2) Run the following spark-submit command :   
spark-submit --master local --deploy-mode client --num-executors 4 --executor-cores 4 --class sayari sayari_sanctions_2.11-0.1.jar "hdfspath/to/ofac.jsonl" "hdfs/path/to/gbr.jsonl"


OUTPUT FILE NAME : 

sanctions_match.xlsx
