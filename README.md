# app-insights-generator
Repo to analyze and extract meaningful insights from mobile application data, focusing on the Google Play Store. Leveraging PySpark for distributed data processing

Steps to run the project into an EMR cluster
1. Create an EMR cluster (Make sure to add rule into security groups to all spark-ui connections).
2. Login to Primary node using SSH
3. Create an empty main.py python file.
4. Copy the code from this repo's main.py code and paste it into Primary node's main.py file.
5. Run command `spark_submit main.py `, here input_file and output_file's path can be given into command line arguments
6. Wait for the spark process to complete
7. It will print output file's path at the end of the logs.
8. Go to the given path and download the file with .csv extension

Points to be taken into consideration - 
1. Certain functionalities which are not in the scope of the code, may lack comprehensive handling
    1. The use of print statements instead of better logging mechanisms
   2. An assumption that the output path will always be empty, without considering scenarios where data may already exist at the specified location.
   3. Lack of a structured approach for handling variables and functions across multiple directories
   4. Defaulting of input and output paths
   5. Certain other variable are hardcoded too.
   6. Input CSV file was stored on S3 
   7. Assumed that all of the numeric values will be in integer range

