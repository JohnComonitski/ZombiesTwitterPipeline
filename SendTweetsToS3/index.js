const{ Client } = require('pg');
const AWS = require('aws-sdk')
const S3 = new AWS.S3();
require('dotenv').config();

exports.handler = async (event, context) => {
    context.callbackWaitsForEmptyEventLoop = false;
    const client = new Client({
        connectionString: process.env.PG
    });
    await client.connect();

    var q = `SELECT * from public.tweets WHERE checked='false'`
    var tweets = await client.query(q)
    tweets = tweets.rows

    var data = 'tweet_id,tweet,map\n'
    for(var i = 0; i < tweets.length; i++){
        var tweet = tweets[i]
        data += `${tweet.id},${cleanText(tweet.tweet)},${tweet.map}\n`
        var q = `UPDATE public.tweets set checked='true' WHERE id='${tweet.id}'`
        await client.query(q)
    }

    var datetime = new Date();
    var fileName = `tweets${datetime.toISOString().slice(0,10)}.csv`

    const params = {
        Bucket:'tweets-to-be-processed',
        Key: fileName,
        Body: data,
        ContentType: 'text/csv; charset-utf-8'
    }
    await S3.putObject(params).promise()

    console.log(fileName)
    client.end();

    function cleanText(text){
        //Remove Line Breaks
        while(text.indexOf("\n") != -1){
            text = text.replace("\n"," ")
        }
        //Remove Commas
        while(text.indexOf(",") != -1){
            text = text.replace(",","")
        }
        //Remove Hashtags
        while(text.indexOf("#") != -1){
            text = text.replace("#","")
        }
        //Remove '
        while(text.indexOf("'") != -1){
            text = text.replace("'","")
        }
        //Remove quote
        while(text.indexOf("\"") != -1){
            text = text.replace("\"","")
        }

        return text
    }
}
