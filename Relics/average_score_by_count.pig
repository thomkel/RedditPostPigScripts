REGISTER /usr/share/cassandra/apache-cassandra.jar;
REGISTER /usr/share/cassandra/apache-cassandra-thrift-2.0.11.jar;
REGISTER /usr/share/cassandra/lib/jamm-0.2.5.jar;
REGISTER /usr/share/cassandra/lib/libthrift-0.9.1.jar;
REGISTER ./cassandra-driver-core-2.0.5.jar;

POSTS = LOAD '/inputs/post_data/part*' USING PigStorage(',')
  as (image_id: int, unix_time:int, rawtime, title, total_votes:int, reddit_id, upvotes:int, 
	subreddit, downvotes:int, local_time:int, score:int, comments:int, username);

GROUP_POSTS = GROUP POSTS BY image_id;
COUNTS = FOREACH GROUP_POSTS GENERATE COUNT($1.image_id) AS count, SUM($1.score) AS score;
GROUP_COUNTS = GROUP COUNTS BY count;
COUNT_OF_COUNTS = FOREACH GROUP_COUNTS GENERATE group AS count, COUNT($1.count) as num_counts;
SCORE_BY_COUNTS = FOREACH GROUP_COUNTS GENERATE group AS count, SUM($1.score) as sum_score;
JOIN_COUNTS = JOIN COUNT_OF_COUNTS BY $0, SCORE_BY_COUNTS BY $0;
AVG_COUNT = FOREACH JOIN_COUNTS GENERATE $0, ($3 / $1) / $0;

DUMP AVG_COUNT;

-- STORE CASSANDRA_STRUCTURED INTO
--  'cql://flights/route_delays?output_query=update route_delays set delay%3D%3F'
--  USING org.apache.cassandra.hadoop.pig.CqlStorage();