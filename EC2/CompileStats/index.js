const{ Client } = require('pg');
const redis = require('redis')
require('dotenv').config();
const fs = require('fs');

getStats();
var date = new Date().toISOString().slice(0, 10);
getStatsByDate("2022-06-10")

async function getStats(){
    //Connect
    const connectionString = process.env.PG;
    const client = new Client({
        connectionString: connectionString
    });
    await client.connect();

    console.log("Collecting Tweets")
    //Get Tweets
    var q = `SELECT * from public.checked_tweets`
    var tweets = await client.query(q)
    tweets = tweets.rows
    client.end();

    console.log("Tweets Collected")

    var emojis = ["🛢","😵‍💫","🇯🇵","🏭","🎥","5️⃣", "🚀","🥶","🌴","🌑", "🚍","💣", "🌇","👮‍♂️","🤠","🤖","🐙","🏹","🕷","🐉","🦆","🚢","9️⃣","🔑","🤫","🐺","🇬🇷","☢️","☃️","⚙️","🔥"]
    var mapNames = ["nacht der untoten","verruckt","shi no numa","der riese","kino der toten", "five", "ascension","call of the dead","shangri-la","moon", "tranzit","nuketown","die rise","mob of the dead","buried","origins","shadows of evil","der eisendrache", "zetsubou no shima","gorod krovi","revelations","voyage of despair","ix","blood of the dead","classified","dead of the night","ancient evil","alpha omega","tag der toten","die maschine","firebase z"]
    var stats = {}

    for(var i = 0; i < tweets.length; i++){
        var tweet = tweets[i]
        if(stats[mapNames[tweet.map]] === undefined){
            stats[mapNames[tweet.map]] = {
                tweets: [],
                count: 0,
                positive: 0,
                negative: 0,
                reception: 0,
                likes: 0,
                dislikes: 0,
                emoji: emojis[tweet.map]
            }
        }

        var tmp = stats[mapNames[tweet.map]]
        if(tweet.positive == 0){
            tmp["reception"] = tmp["reception"] + 1
            tmp["positive"] = tmp["positive"] + 1
            tmp["count"] = tmp["count"] + 1 
            tmp["likes"] = tmp["likes"] + tweet.likes + 1
        }
        else if(tweet.positive == 2){
            tmp["reception"] = tmp["reception"] - 1
            tmp["negative"] = tmp["negative"] + 1
            tmp["count"] = tmp["count"] + 1 
            tmp["dislikes"] = tmp["dislikes"] + tweet.likes + 1
        }
        if(!tmp.tweets.includes(tweet.id)){
            tmp.tweets.push(tweet.id)
        }

        stats[mapNames[tweet.map]] = tmp
    }

    //Get Confidence
    for (const map in stats){
        stats[map] = {confidence: getConfidenceInterval(stats[map]), ...stats[map]}
    }

    await sendToRedis("stats",stats)
}

async function getStatsByDate(date){
    //Connect
    const connectionString = process.env.PG;
    const client = new Client({
        connectionString: connectionString
    });
    await client.connect();

    console.log("Collecting Tweets for " + date)
    //Get Tweets
    var q = `SELECT * from public.checked_tweets where date='${date}'`
    var tweets = await client.query(q)
    tweets = tweets.rows
    client.end();

    console.log("Tweets Collected for " + date)

    var emojis = ["🛢","😵‍💫","🇯🇵","🏭","🎥","5️⃣", "🚀","🥶","🌴","🌑", "🚍","💣", "🌇","👮‍♂️","🤠","🤖","🐙","🏹","🕷","🐉","🦆","🚢","9️⃣","🔑","🤫","🐺","🇬🇷","☢️","☃️","⚙️","🔥"]
    var mapNames = ["nacht der untoten","verruckt","shi no numa","der riese","kino der toten", "five", "ascension","call of the dead","shangri-la","moon", "tranzit","nuketown","die rise","mob of the dead","buried","origins","shadows of evil","der eisendrache", "zetsubou no shima","gorod krovi","revelations","voyage of despair","ix","blood of the dead","classified","dead of the night","ancient evil","alpha omega","tag der toten","die maschine","firebase z"]
    var stats = {}

    for(var i = 0; i < tweets.length; i++){
        var tweet = tweets[i]
        if(stats[mapNames[tweet.map]] === undefined){
            stats[mapNames[tweet.map]] = {
                tweets: [],
                count: 0,
                positive: 0,
                negative: 0,
                reception: 0,
                likes: 0,
                dislikes: 0,
                emoji: emojis[tweet.map]
            }
        }

        var tmp = stats[mapNames[tweet.map]]
        if(tweet.positive == 0){
            tmp["reception"] = tmp["reception"] + 1
            tmp["positive"] = tmp["positive"] + 1
            tmp["count"] = tmp["count"] + 1 
            tmp["likes"] = tmp["likes"] + tweet.likes + 1
        }
        else if(tweet.positive == 2){
            tmp["reception"] = tmp["reception"] - 1
            tmp["negative"] = tmp["negative"] + 1
            tmp["count"] = tmp["count"] + 1 
            tmp["dislikes"] = tmp["dislikes"] + tweet.likes + 1
        }
        if(!tmp.tweets.includes(tweet.id)){
            tmp.tweets.push(tweet.id)
        }

        stats[mapNames[tweet.map]] = tmp
    }

    //Get Confidence
    for (const map in stats){
        stats[map] = {confidence: getConfidenceInterval(stats[map]), ...stats[map]}
    }

    await sendToRedis(date+"-stats", stats)
}

function getConfidenceInterval(map){
    var avg = (map.positive)/(map.count)
    var adjustedAvg = (map.positive+2)/(map.count+4)
    var error = Math.sqrt(((1 - adjustedAvg)/(map.count+4)))
    var marginOfError = 2 * error
    var interval = [avg+marginOfError, avg-marginOfError]

    return interval
}

async function sendToRedis(key,stats){
    var redis_url = process.env.REDIS_URL
    const client = redis.createClient({
        url: redis_url
    })
    await client.connect()
    client.json.set(key, '.', stats)
    console.log('Stats Updated')

    client.quit();
}
