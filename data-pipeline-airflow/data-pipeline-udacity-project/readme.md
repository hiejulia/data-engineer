**AWS Connection**
Conn Id: Enter aws_credentials.
Conn Type: Enter Amazon Web Services.
Login: Enter your Access key ID from the IAM User credentials you downloaded earlier.
Password: Enter your Secret access key from the IAM User credentials you downloaded earlier.

**Redshift Connection**
Conn Id: Enter redshift.
Conn Type: Enter Postgres.
Host: Enter the endpoint of your Redshift cluster, excluding the port at the end. 
Schema: This is the Redshift database you want to connect to.
Login: Enter awsuser.
Password: Enter the password created when launching the Redshift cluster.
Port: Enter 5439.


- Data source 
    - log data : `s3://udacity-dend/log_data`
    - song data : `s3://udacity-dend/song_data`