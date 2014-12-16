REGISTER /usr/share/cassandra/apache-cassandra.jar;
REGISTER /usr/share/cassandra/apache-cassandra-thrift-2.0.11.jar;
REGISTER /usr/share/cassandra/lib/jamm-0.2.5.jar;
REGISTER /usr/share/cassandra/lib/libthrift-0.9.1.jar;
REGISTER ./cassandra-driver-core-2.0.5.jar;

POSTS = LOAD '/inputs/post_data/part*' USING PigStorage(',')
  as (image_id: int, unix_time:int, rawtime, title, total_votes:int, reddit_id, upvotes:int, 
	subreddit, downvotes:int, local_time:int, score:int, comments:int, username);

POSTS_BY_IMAGE_ID = GROUP POSTS BY image_id;
AVERAGE_SCORE = FOREACH POSTS_BY_IMAGE_ID GENERATE group AS image_id, AVG($1.total_votes) AS votes, 
	AVG($1.score) AS score;
CASSANDRA_STRUCTURED = FOREACH AVERAGE_SCORE GENERATE TOTUPLE(TOTUPLE('image_id', image_id)), 
	TOTUPLE(score), TOTUPLE(votes);

DUMP CASSANDRA_STRUCTURED;

-- STORE CASSANDRA_STRUCTURED INTO
--  'cql://flights/route_delays?output_query=update route_delays set delay%3D%3F'
--  USING org.apache.cassandra.hadoop.pig.CqlStorage();