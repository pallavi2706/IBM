# IBM Exercise
Exercise:
Install and configure Eclipse using the other instructions provided, and then use Eclipse to develop a
Scala program/project (using Scala and Spark) which implements the following functions/features:
1. Create a COS instance on IBM Cloud Object Store (COS) and place the attached data file there.
2. Function that reads a CSV file (emp-data.csv) from the COS bucket
3. Setup a DB2 database
a. Setup an account in IBM Public Cloud (cloud.ibm.com)
b. Create a simple DB2 database
c. HINT: you will need DB2 JDBC drivers for future steps
i. (https://www.ibm.com/support/pages/db2-jdbc-driver-versions-and-downloads)
4. Write Scala code to:
a. Create a table based COS data schema read in Step 2
b. Write the contents from Step 2 to the table
5. Write Scala code to read the same data from the database, calculate and display the following:
a. Gender ratio in each department
b. Average salary in each department
c. Male and female salary gap in each department
6. Scala code to write one the calculated data as a Parquet back to COS
