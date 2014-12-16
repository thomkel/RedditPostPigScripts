-- register commands for local install done through datastax deb.
REGISTER /usr/share/cassandra/apache-cassandra.jar;
REGISTER /usr/share/cassandra/apache-cassandra-thrift-2.0.11.jar;
REGISTER /usr/share/cassandra/lib/jamm-0.2.5.jar;
REGISTER /usr/share/cassandra/lib/libthrift-0.9.1.jar;
REGISTER ./cassandra-driver-core-2.0.5.jar;

REDDIT_POSTS = LOAD '/inputs/post_summary' USING PigStorage(',')
as (reddit_id, image_id: int, title, subreddit, comments: int, username, unix_time:int, rawtime, localtime:int, 
	score:int, total_votes: int, upvotes: int, downvotes: int);

POSTS_BY_SUBREDDIT = GROUP REDDIT_POSTS BY subreddit;
SUM_VOTES = FOREACH POSTS_BY_SUBREDDIT GENERATE group AS subreddit, COUNT($1.subreddit) as count,
	SUM($1.total_votes) AS votes, SUM($1.upvotes) AS upvotes, SUM($1.downvotes) AS downvotes;
CASSANDRA_STRUCTURED = FOREACH SUM_VOTES GENERATE TOTUPLE(TOTUPLE('subreddit', subreddit)), 
	TOTUPLE(count, votes, upvotes, downvotes);

STORE CASSANDRA_STRUCTURED INTO
  'cql://reddit/votes_by_subreddit?output_query=update votes_by_subreddit set count%3D%3F,votes%3D%3F,upvotes%3D%3F,downvotes%3D%3F' USING org.apache.cassandra.hadoop.pig.CqlStorage();
