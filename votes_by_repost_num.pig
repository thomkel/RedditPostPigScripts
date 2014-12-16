-- register commands for local install done through datastax deb.
REGISTER /usr/share/cassandra/apache-cassandra.jar;
REGISTER /usr/share/cassandra/apache-cassandra-thrift-2.0.11.jar;
REGISTER /usr/share/cassandra/lib/jamm-0.2.5.jar;
REGISTER /usr/share/cassandra/lib/libthrift-0.9.1.jar;
REGISTER ./cassandra-driver-core-2.0.5.jar;
REGISTER /usr/share/cassandra/lib/datafu-1.2.0.jar;
define Enumerate datafu.pig.bags.Enumerate('1');

REDDIT_POSTS = LOAD '/inputs/post_summary' USING PigStorage(',')
as (reddit_id, image_id: int, title, subreddit, comments: int, username, unixtime:int, rawtime, localtime:int, 
	score:int, total_votes: int, upvotes: int, downvotes: int);

POSTS_BY_IMAGE_ID = GROUP REDDIT_POSTS BY image_id;
ORDER_POSTS = FOREACH POSTS_BY_IMAGE_ID {
	sorted = ORDER REDDIT_POSTS BY unixtime ASC;
	GENERATE group, sorted;
}
FLATTENED = FOREACH ORDER_POSTS GENERATE FLATTEN(Enumerate(sorted));
STRUCTURED = foreach FLATTENED generate $13 as count, $0 as reddit_id, $1 as image_id, 
	$6 as unixtime, $10 as votes, $11 as upvotes, $12 as downvotes;
CASSANDRA_STRUCTURED = FOREACH STRUCTURED GENERATE TOTUPLE(TOTUPLE('count', count), 
	TOTUPLE('image_id', image_id)), TOTUPLE(reddit_id, unixtime, votes, upvotes, downvotes);
STORE CASSANDRA_STRUCTURED INTO
  'cql://reddit/votes_by_repost_num_raw?output_query=update votes_by_repost_num_raw set reddit_id%3D%3F,unixtime%3D%3F,votes%3D%3F,upvotes%3D%3F,downvotes%3D%3F' USING org.apache.cassandra.hadoop.pig.CqlStorage();

GROUPED = GROUP STRUCTURED BY count;
GROUPED_COUNTED = FOREACH GROUPED GENERATE group AS count, 
	SUM($1.votes) AS votes, SUM($1.upvotes) AS upvotes, SUM($1.downvotes) AS downvotes;
CASSANDRA_STRUCTURED = FOREACH GROUPED_COUNTED GENERATE TOTUPLE(TOTUPLE('count', count)), 
	TOTUPLE(votes, upvotes, downvotes);
STORE CASSANDRA_STRUCTURED INTO
  'cql://reddit/votes_by_repost_num?output_query=update votes_by_repost_num set votes%3D%3F,upvotes%3D%3F,downvotes%3D%3F' USING org.apache.cassandra.hadoop.pig.CqlStorage();

