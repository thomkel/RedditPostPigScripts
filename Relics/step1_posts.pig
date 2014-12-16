    -- Import the CSVLoader plugin (available from the 3rd party piggybank modules collection)
    register ./contrib/piggybank/java/piggybank.jar;

    -- Load our reddit post data from HDFS
    RAW_POST_DATA = LOAD '/tmp/postData/*' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER'); 

    POST_DATA = FOREACH RAW_POST_DATA GENERATE $0 AS image_id, $1 AS unixtime, $2 AS raw_time, $3 as title, $4 AS total_votes, 
	$5 AS reddit_id, $6 AS upvotes, $7 AS subreddit, $8 AS downvotes, $9 AS localtime, $10 AS score, $11 AS comments, $12 AS username;
    
    -- Filter out post data that are missing the fields we are interested in
    POST_DATA_FILTERED = FILTER POST_DATA BY (image_id neq '') AND (unixtime neq '') AND (reddit_id neq '') AND (score neq '')
	AND (upvotes neq '') AND (total_votes neq '') AND (downvotes neq '');

    -- Store the resulting data into HDFS as a CSV file
    STORE POST_DATA_FILTERED INTO '/inputs/post_data/' USING PigStorage(',');

