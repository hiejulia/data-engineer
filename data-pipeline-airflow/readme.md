- config airflow 
- AIRFLOW_HOME and AIRFLOW_CONFIG 

- place dags & plugins on airflow_home dir 



2. Initialize Airflow data base with `airflow initdb`, and open webserver with `airflow webserver`
3. Access the server `http://localhost:8080` and create:

- airflow GUI 


- airflow structure
    - dags/...
    - plugins/...


- DAG param 
    * Owner
    * Depends_on_past
    * Start_date
    * Retries
    * Retry_delay
    * Catchup


### Operators
Operators create necessary tables, stage the data, transform the data, and run checks on data quality.

Connections and Hooks are configured using Airflow's built-in functionalities.

All of the operators and task run SQL statements against the Redshift database. 

#### Stage Operator
The stage operator loads any JSON and CSV formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.

- **Task to stage CSV and JSON data is included in the DAG and uses the RedshiftStage operator**: There is a task that to stages data from S3 to Redshift. (Runs a Redshift copy statement)

- **Task uses params**: Instead of running a static SQL statement to stage the data, the task uses params to generate the copy statement dynamically. It also contains a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.

- **Logging used**: The operator contains logging in different steps of the execution

- **The database connection is created by using a hook and a connection**: The SQL statements are executed by using a Airflow hook

#### Fact and Dimension Operators
The dimension and fact operators make use of the SQL helper class to run data transformations. Operators take as input the SQL statement from the helper class and target the database on which to run the query against. A target table is also defined that contains the results of the transformation.

Dimension loads are done with the truncate-insert pattern where the target table is emptied before the load. There is a parameter that allows switching between insert modes when loading dimensions. Fact tables are massive so they only allow append type functionality.

- **Set of tasks using the dimension load operator is in the DAG**: Dimensions are loaded with on the LoadDimension operator

- **A task using the fact load operator is in the DAG**: Facts are loaded with on the LoadFact operator

- **Both operators use params**: Instead of running a static SQL statement to stage the data, the task uses params to generate the copy statement dynamically

- **The dimension task contains a param to allow switch between append and insert-delete functionality**: The DAG allows to switch between append-only and delete-load functionality

#### Data Quality Operator
The data quality operator is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result are checked and if there is no match, the operator raises an exception and the task is retried and fails eventually.

For example one test could be a SQL statement that checks if certain column contains NULL values by counting all the rows that have NULL in the column. We do not want to have any NULLs so expected result would be 0 and the test would compare the SQL statement's outcome to the expected result.

- **A task using the data quality operator is in the DAG and at least one data quality check is done**: Data quality check is done with correct operator

- **The operator raises an error if the check fails**: The DAG either fails or retries n times

- **The operator is parametrized**: Operator uses params to get the tests and the results, tests are not hard coded to the operator


### Airflow UI views of DAG and plugins

The dag follows the data flow provided in the instructions, all the tasks have a dependency and DAG begins with a start_execution task and ends with a end_execution task.