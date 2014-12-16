REGISTER ./elephant-bird-core-4.5.jar;
REGISTER ./elephant-bird-pig-4.5.jar;
REGISTER ./elephant-bird-hadoop-compat-4.5.jar;
REGISTER ./libthrift-0.9.0.jar;
REGISTER ./contrib/piggybank/java/piggybank.jar;
REGISTER /home/mpcs/workspace/PostSummary/target/PostSummary-0.0.1-SNAPSHOT.jar; 
DEFINE ThriftBytesToTupleDef com.twitter.elephantbird.pig.piggybank.ThriftBytesToTuple('edu.uchicago.mpcs53013.PostSummary.PostSummary');
POSTS = LOAD '/inputs/post_data/part*' USING PigStorage(',')
  as (reddit_id, image_id: int, title, subreddit, comments: int, username, unix_time:int, rawtime, localtime:int, 
	score:int, total_votes: int, upvotes: int, downvotes: int);

RAW_DATA = LOAD '/inputs/thriftPosts/posts' USING org.apache.pig.piggybank.storage.SequenceFileLoader() as (key:long, value: bytearray);
POST_SUMMARY = FOREACH RAW_DATA GENERATE FLATTEN(ThriftBytesToTupleDef(value));
STORE POST_SUMMARY into '/inputs/post_summary' Using PigStorage(',');
