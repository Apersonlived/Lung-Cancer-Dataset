The data has been extracted from Kaggle Lung Cancer Dataset,
https://www.kaggle.com/datasets/thedevastator/cancer-patients-and-air-pollution-a-new-link 

To run the etl pipeline, first clone the GitHub repository unto your computer and store it in a folder. For this, go to the folder you want to clone the GitHub repository to and run the below command in the terminal.
cd directory
git clone https://github.com/Apersonlived/Lung-Cancer-Dataset.git

For pyspark and jar files installation refer to: 
https://github.com/neotheobserver/summerclass-bigdataandpyspark 
Refer to branch Week 2 for pyspark installation and further setup.

We are going to extract the data into our LungData folder (you can use any relevant folder name) by executing our extrac/execute.py file. Use python3 for execution,
python3 extract/execute.py /home/LungData/extract
Make sure you are inside the relevant directory to execute the file.

The data is then transformed by removing irrelevant or null values. The code is given in the git repository itself. 
The transform stage is divided into two stages. In Stage 1, the data is loaded and cleaned and stored in patients table and in the second stage a table is made with relevant datas. 
To execute the transform statement,
python3 transform/execute.py /home/LungData/extract /home/LungData/transform

For the loading the data to PostgreSQL, firstly you need to download PostgreSQL, pgadmin (to do it with GUI, you can also use the command line for this) and the driver.
For the downloads refer to the repository above.

Open pgadmin and create the initial setup of host and port. Also, create a new database to load your dataset to.
After all the permissions and database setup is done, execute the load file. 
python3 transform/execute.py /home/LungData/extract /home/LungData/transform username password host database port
repladce the execution statement with information relevant to your database setup.

In this project I am working with superset to visualize the data. For the setup of superset you can again refer to the git repository above:
https://github.com/neotheobserver/summerclass-bigdataandpyspark 
Refer to week 4

Now we can move towards Data Visualization,
Connect the database to the superset and create a dashboard for it, create various charts relevant for the project.
Based on the columns we are going to visualize the data for Lung Cancer on various metrics.

Try creating various visualizations of the dataset according to their relevancy.
