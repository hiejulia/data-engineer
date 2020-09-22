REGISTER 'udfs/my_first_udf.py' USING streaming_python AS pyudfs;

A = LOAD '../resources/input.txt';
B = FOREACH A GENERATE pyudfs.return_one();
DUMP B;
-- $ pig -x local simple_udf.pig
-- ...
-- (1)
-- (1)
-- (1)