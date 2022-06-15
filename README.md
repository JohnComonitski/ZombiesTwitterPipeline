# ZombiesTwitterPipeline

Scripts powering the backend of Zombies Twitter

## Step 1) Tweet Collector
AWS Lambda Twitter bot to collect and filter tweets everyday.

## Step 2) Send Tweets to S3
AWS Lambda Function to take the latest unchecked tweets, clean them and send to S3 in a CSV.

## Step 3) Analyze Tweets
Python script running in an EC2 instance every Friday to pull the latest tweets from an S3 bucket, analyze those tweets and store the results in our maindate base and Redis cache.

## Step 4) Compile Stats 
Node JS script running in an EC2 instance after the latest tweets are analyzed. The script compiles and updates the stats/ranking with the new tweets.
