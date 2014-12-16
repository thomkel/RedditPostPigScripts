RedditPostPigScripts
====================

Batch Views created with Pig Scripts and stored in Cassandra

Two pig scripts, step1_posts.pig and step2_posts.pig, were used to scrape the data and store in PigStorage. Only step2_posts.pig needs to be run as step1_posts.pig is a relic no longer used.

Four pig scripts used to create batch views were used in the final product:
1.	votes_by_group_id.pig- posts are grouped by image_id and number of posts, sum of total_votes, sum of upvotes, sum of downvotes are calculated
2.	votes_by_repost_num.pig- posts are grouped by image_id, sorted by unixtime, and then ranked and enumerated. This data is stored in a Cassandra table as the raw data. This data is then grouped by rank and the votes, upvotes and downvotes are summed and stored in another table.
3.	votes_by_subreddit.pig- posts are grouped by subreddit and number of posts, sum of total_votes, upvotes and downvotes are calculated
4.	store_repost.pig- simple table that maps reddit_ids to image_ids

Relics: average_score_by_count.pig and average_score_by_image_id.pig were not used because it was decided to do average calculations in the front end application
