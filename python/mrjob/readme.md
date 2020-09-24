- pip install mrjob

Run 
python mr-mr_s3_log_parser.py -r emr s3://bucket-source/ --output-dir=s3://bucket-dest/



Run locally 
python mr_s3_log_parser.py input_data.txt > output_data.txt



Run test 
python test_mr_s3_log_parser.py -v

.mrjob.conf



