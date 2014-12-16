-- register commands for local install done through datastax deb.
REGISTER /usr/share/cassandra/apache-cassandra.jar;
REGISTER /usr/share/cassandra/apache-cassandra-thrift-2.0.11.jar;
REGISTER /usr/share/cassandra/lib/jamm-0.2.5.jar;
REGISTER /usr/share/cassandra/lib/libthrift-0.9.1.jar;
REGISTER ./cassandra-driver-core-2.0.5.jar;

REDDIT_POSTS = LOAD '/inputs/post_summary' USING PigStorage(',')
as (reddit_id, image_id: int, title, subreddit, comments: int, username, unix_time:int, rawtime, localtime:int, 
	score:int, total_votes: int, upvotes: int, downvotes: int);
REPOST_INFO = FOREACH REDDIT_POSTS GENERATE reddit_id, image_id;
CASSANDRA_STRUCTURED  = FOREACH REPOST_INFO GENERATE TOTUPLE(TOTUPLE('reddit_id', reddit_id)), TOTUPLE(image_id);

STORE CASSANDRA_STRUCTURED INTO
  'cql://reddit/repost_info?output_query=update repost_info set image_id%3D%3F'
  USING org.apache.cassandra.hadoop.pig.CqlStorage();