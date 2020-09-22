REGISTER 'udfs/myudf.py' USING streaming_python AS my_udf;

relation = FOREACH data GENERATE my_udf.function(field);

