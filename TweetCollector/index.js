var Twit = require("twitter");
const{Client} = require('pg');
var config = require('./config');
var data = require('./maps');
require('dotenv').config();

function formatDate(date) {
    var d = new Date(date),
        month = '' + (d.getMonth() + 1),
        day = '' + d.getDate(),
        year = d.getFullYear();

    if (month.length < 2)
        month = '0' + month;
    if (day.length < 2)
        day = '0' + day;

    return [year, month, day].join('-');
}

function checkForFilterWords(tweet){
    var filteredWords = ["@Treyarch","?","Guide","Play","Played","Playing","Record","Wr","Speedrun","Round","Step","Run","Exo","Advanced warfare","CW","CW Zombies","Cold War","Outbreak","Pill","Vs","Blue","Red","Der Anfang","Tonight","Vanguard","Campaign","Mission","Streams","Im on Der Riese","Boss","Boss fight","Died","Remaster","Speed run","Speedrun","remastered","Real life","Irl","Step","Died","Reset","Down","Downed","Song","Musical","Music video"]
    for (var i = 0; i < filteredWords.length; i++){
        if(tweet.toLowerCase().indexOf(filteredWords[i].toLowerCase()) !== -1){
            return false
        }
    }
    return true;
}

function isTweetLongEnough(tweet){
    //Remove Numbers, commas and line breaks
    tweet = tweet.replace("\n","").replace(",","").replace(/[0-9]/g, '')
    //Remove @s and Map Names
    var tmp = removeMap(removeUser(tweet))
    //Check word count
    if(countWords(tmp) >= 4){
        return true
    }
    return false
}

function removeMap(tweet){
    var tweet = tweet.toLowerCase();
    var mapNames = ["nacht der untoten","verruckt","verrückt","shi no numa","der riese","kino der toten","five", "ascension","call of the dead","shangri-la","moon","tranzit","die rise","mob of the dead","buried","origins","shadows of evil","der eisendrache","zetsubou no shima","gorod krovi","revelations","blood of the dead","classified","voyage of despair","ix","dead of the night","ancient evil","alpha omega","tag der toten","die maschine","firebase z"]
    for(var i = 0; i < mapNames.length; i++){
        tweet = tweet.replace(mapNames[i], "")
    }
    return tweet;
}

function removeUser(tweet){
    while (tweet.indexOf("@") !== -1){
        var index = tweet.indexOf("@");
        var count = 1;
        while((index + count) < tweet.length && tweet.charAt(index + count) !== " "){
          count += 1;
        }
        tweet = tweet.substring(0, index) + tweet.substring(index + count);
    }
    return tweet;

}

function countWords(str) {
  var matches = str.match(/[\w\d\’\'-]+/gi);
  return matches ? matches.length : 0;
}

var count = 0;
async function getTweets(map){
    //Connect to DB
	const connectionString = process.env.PG
    const client = new Client({
        connectionString: connectionString
    });
    await client.connect();

    //Twitter Client
    var T = new Twit(config);

	var mapDetails = map.split(",");

    //Tweet
    var tweet = {
        id: "",
        tweet: "",
        likes: 0,
        rts: 0,
        user: "",
        profile: "",
        at: "",
        img: "",
        positive: false,
        checked: false,
        positive_bot: false,
        map: mapDetails[0],
        relevant: false
    }
    var today = new Date();
	var yesterday = new Date(today);
	yesterday.setDate(yesterday.getDate() - 1);
	yesterday = formatDate(yesterday);

    var query  = mapDetails[2].substring(0,mapDetails[2].length-1) + ` since:${yesterday}`;
	var params = {
        q: query,
        maxResults: 500,
        result_type: 'recent',
        lang: 'en',
		tweet_mode: 'extended'
    };
    

	const myPromise = new Promise((resolve, reject) => {
		T.get('search/tweets', params, async function (err, data, response) {
            if(!err){
              console.log(data.statuses.length);
    
              for(var i = 0; i < data.statuses.length; i++){
                var tmp = data.statuses[i];
                
                tweet.id = tmp.id_str;
                if(tmp.retweeted_status != undefined){
                	tweet.tweet = tmp.retweeted_status.full_text;
                }
                else{
                	tweet.tweet = tmp.full_text;
                }
                var valid = (checkForFilterWords(tweet.tweet) && isTweetLongEnough(tweet.tweet))
                //console.log("Is Tweet Valid:",valid)
                //console.log(tweet.tweet);
                if(valid)
                {
                    tweet.likes = tmp.favorite_count;
                    tweet.rts = tmp.retweet_count;
                    tweet.at = tmp.user.screen_name;
                    tweet.user = tmp.user.name.replace("#","").replace("'","")
                    tweet.img = tmp.user.profile_image_url_https
                    tweet.profile = tmp.user.id_str;
                    
                    var q = "SELECT * from public.tweets WHERE id = \'" + tweet.id + "\'";
                    var check = await client.query(q);
                    if(check.rowCount == 0){
                    	q = `INSERT INTO public.tweets (tweet, likes, rts, username, profile, positive, checked, positive_bot, map, relevant, id, at, img)
                      VALUES('${tweet.tweet.replace("'","")}',${tweet.likes},${tweet.rts},'${tweet.user}','${tweet.profile}','${tweet.positive}','${tweet.checked}','${tweet.positive_bot}',${tweet.map},'${tweet.relevant}','${tweet.id}', '${tweet.at}', '${tweet.img}')
                      `
                      try{
                        //console.log(tweet.tweet)
                        count++;
                        await client.query(q);
                      }
                      catch(e){
                          console.log("ERROR: Did not add tweet:",q)
                      }
                    }
                    else{
                        //var q = `delete from public.tweets where id='${tweet.id}'`
                        //console.log(q)
                        //await client.query(q);
                        //console.log(data.statuses[i])
                    }
                }
              }
            }
            else{
              console.log(err);
            }
            client.end();

    	});
	});
}

async function main(){
	var maps = data.maps;
    //maps.length-1
	for(var i = 0; i < maps.length-1; i++){
		await getTweets(maps[i]);
	}
	console.log("Total Tweets added:", count)
}

exports.handler = async (event) => {
	await main();
	
    const response = {
        statusCode: 200,
        body: JSON.stringify(''),
    };
    return response;
};