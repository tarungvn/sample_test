# How to run
1. Run the code by using maven command clean compile install package
2. In target folder jar will be created
3. Provide  configuration parameters in json file
4. Provide args in spark submit command which were present in main class


# About the Code
1. It will read data from API
2. Since API data is in json string we convert it into a dataframe
3. Load it into HDFS
4. Create HIve External Table
