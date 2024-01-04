Prerequirements-
Make sure to place the two dataset folders inside the 'Code' subfolder for the script to work.Please check the  Prerequirement.png image file inside code subfolder as a reference on checking whether dataset subfolders were placed correctly. Do not remove any files from the code subfolder as they are a requirement for successful execution. 


Step 1- DATAVAULT DATABASE CREATION
The datavault.sql file in code subfolder contains sql queries to drop smdvault database if exists and then create a new smdvault database. This file needs to be run first in order for the python script to successfully connect with database.
You can Run the datavault.sql file in one of the two following methods-

>>Run the datavault.sql file in sql shell using the \i command followed by the file path enclosed in single quotes and separated by  forward slants like the following. Change the path to your file path.
example command-
\i 'C:/Users/arjun/Documents/submission/code/datavault.sql'
Be sure to follow the command like given above and change the path to the path of the datavault.sql file inside code subfolder in your local machine, make sure that your path contains forward slashes and is enclosed in single quotes.
IMPORTANT-Do this first before running staging.py as without a database, we cannot connect to it

>>If unsuccessful in running the datavault.sql file in the above mentioned method, you can run it in PostgreSQL by copying the queries and running them in the given order. Try this only if unsuccessful in the first method and skip this step if successful in running datavault.sql file


Step 2-CONFIG FILE
The config file has all the details for the database connection, check if the details are correct.The python script looks at this file for database connection details. By default it already has details according to project statement, modify this file only if the connection details are different than what is mentioned here already.
IMPORTANT-Do this first before running the staging.py script for successfull connection.

Step 3- Staging.py
Run the staging.py file in spyder or any python IDE only after successful step 1 and step 2 and after placing the 2 datasets folder inside code subfolder. 
Important-All these things must be in the same folder before running the python file  for succesful script execution>>staging.py,staging.sql,config.txt,informationlayerqueries.sql,VMData_Blinded folder,PreAutismData_Blinded folder,datavault.sql

IF the datasets folders were placed properly, this should run now without issues. When the code is starting you should see prompts like- 'Tables creation is starting', and once the code is complete you will see a prompt- 'Code execution is complete'. Wait till you see the code execution is complete statement as this may take sometime since the data loading and plotting takes sometime. After query execution is complete, you will find 3 new files created in code subfolder namely 'Information Layer query results.xlsx','Plot1.png','Plot2.png'.These are results of code execution done in your machine. Alternatively you can also check all the results obtained during code execution in my machine in results subfolder.

IF you are facing some issues running the code because some python packages are not installed then you can install those packages in command prompt using the pip commands.
Do the following only if any packages are not already installed and this step can be skipped if they are already present.
To install openpyxl python package-pip install openpyxl
To install matplotlib package- pip install matplotlib
To install numpy- pip install numpy
To install pandas- pip install pandas
To install psycopg2- pip install psycopg2
If you haven't installed pip, it should be installed first and alternatively conda install commands can also be used in Anaconda Powershell prompt.

Step 4- Results
Once the staging.py script is ran successfully, You can check the  'Information Layer query results.xlsx' file created in code subfolder to see the query output of the script which ran in your machine. Each query output is placed in a separate sheet. You can also see all the results which were obtained during code execution done in my machine in the results subfolder. You can check the tables creations queries in staging.sql file, you can check the query used in information layer in the informationlayerqueries.sql file.

This concluded my Project in SMD, It was an enormous learning oppurtunity.

