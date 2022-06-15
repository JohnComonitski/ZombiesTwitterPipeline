import requests
import pandas as pd
import psycopg2
import torch
import re
import boto3
from datetime import date
from transformers import TextClassificationPipeline
from transformers import BertTokenizer
from transformers import BertForSequenceClassification

def analyzeText(pipe, text):
    res = pipe(text)
    sentiment = max(range(len(res[0])), key=lambda index: res[0][index]['score'])

    return sentiment

def cleanTweet(text):
    #To Lower Case
    text = text.lower()

    #Hide Map Names
    mapNames = ["nacht der untoten","verruckt","verrückt","shi no numa","der riese","kino der toten","five", "ascension","call of the dead","shangri-la","shangri la","moon","tranzit", "nuketown zombies", "nuketown","die rise","mob of the dead","buried","origins",
    "shadows of evil","der eisendrache","zetsubou no shima","gorod krovi","revelations","blood of the dead","classified","voyage of despair","ix","dead of the night","ancient evil","alpha omega","tag der toten","die maschine","firebase z"]
    for map in mapNames:
        text = text.replace(map, "map")

    #Remove @s
    while (text.find("@") != -1):
        index = text.find("@")
        count = 1
        while((index + count) < len(text) and text[(index + count)] != " "):
            count += 1

        text = text[0: index] + text[index + count:]
    
    #final cleaning
    text = re.sub(r'[^\w\s]', '', text)
    text = text.strip()
    
    return text

def multipleMapNames(text):
    text = text.lower()
    text.replace("\n", "")
    count = 0
    mapNames = ["nacht der untoten","verruckt","verrückt","shi no numa","der riese","kino der toten","nuketown","five", "ascension","call of the dead","shangri-la", "shangri la" ,"moon","tranzit", "nuketown zombies", "nuketown","die rise","mob of the dead","buried","origins", "shadows of evil","der eisendrache","zetsubou no shima","gorod krovi","revelations","blood of the dead","classified","voyage of despair","ix","dead of the night","ancient evil","alpha omega","tag der toten","die maschine","firebase z"]
    for map in mapNames:
        if(text.find(map) != -1):
            count += 1
        
    if(count > 1):
        return True
    return False

def hasMapName(text):
    text = text.lower()
    mapNames = ["nacht der untoten","verruckt","verrückt","shi no numa","der riese","kino der toten", "nuketown","five", "ascension","call of the dead","shangri-la","moon","tranzit", "nuketown", "die rise","mob of the dead","buried","origins",
    "shadows of evil","der eisendrache","zetsubou no shima","gorod krovi","revelations","blood of the dead","classified","voyage of despair","ix","dead of the night","ancient evil","alpha omega","tag der toten","die maschine","firebase z"]
    for map in mapNames:
      if text.find(map) != -1:
        return True
    return False

def repleaceMap(text):
  text = text.lower()
  mapNames = ["nacht der untoten","verruckt","verrückt","shi no numa","der riese","kino der toten","five", "nuketown","ascension","call of the dead","shangri-la","shangri la","moon","tranzit", "nuketown zombies", "nuketown", "die rise","mob of the dead","buried","origins","shadows of evil","der eisendrache","zetsubou no shima","gorod krovi","revelations","blood of the dead","classified","voyage of despair","ix","dead of the night","ancient evil","alpha omega","tag der toten","die maschine","firebase z"]
  for map in mapNames: 
      text = text.replace(map, "map")
  return text

def isTextLongEnough(text):
  #Remove Numbers, commas and line breaks
  text = text.replace("\n","").replace(",","")
  text = ''.join([i for i in text if not i.isdigit()])
  #Remove @s and Map Names
  tmp = repleaceMap(text)
  #Check word count
  if(len(tmp.split()) > 2):
      return True
  return False

def handleMultiMapTweet(text):
    breakdown = text.split(".")
    tweets = [""]
  
    for i in range(len(breakdown)):
      #check is text is long enough and has a map nap
      if(hasMapName(breakdown[i]) and isTextLongEnough(breakdown[i])):
        if(hasMapName(tweets[len(tweets)-1]) and isTextLongEnough(tweets[len(tweets)-1])):
          tweets.append(breakdown[i])        
        else:
          tweets[len(tweets)-1] = tweets[len(tweets)-1] + breakdown[i] 
      else:
        tweets[len(tweets)-1] = tweets[len(tweets)-1] + breakdown[i] 
    
    return tweets

def getMaps(tweet):
  mapsMaping = {
    "nacht der untoten":0,
    "verruckt":1,
    "verrückt":1,
    "shi no numa":2,
    "der riese":3,
    "kino der toten":4,
    "five":5,
    "ascension":6,
    "call of the dead":7,
    "shangri-la":8,
    "shangri la":8,
    "moon":9,
    "tranzit":10,
    "nuketown":11,
    "nuketown zombies":11,
    "die rise":12,
    "mob of the dead":13,
    "buried":14,
    "origins":15,
    "shadows of evil":16,
    "der eisendrache":17,
    "zetsubou no shima":18,
    "gorod krovi":19,
    "revelations":20,
    "voyage of despair":21,
    "ix":22,
    "blood of the dead":23,
    "classified":24,
    "dead of the night":25,
    "ancient evil":26,
    "alpha omega":27,
    "tag der toten":28,
    "die maschine":29,
    "firebase z": 30
  }
  mapNames = ["nacht der untoten","verruckt","verrückt","shi no numa","der riese","kino der toten","five", "ascension","call of the dead","shangri-la","shangri la","moon","tranzit","nuketown","die rise","mob of the dead","buried","origins","shadows of evil","der eisendrache","zetsubou no shima","gorod krovi","revelations","blood of the dead","classified","voyage of despair","ix","dead of the night","ancient evil","alpha omega","tag der toten","die maschine","firebase z"]
  maps = []
  for map in mapNames:
      if(tweet.lower().find(map) != -1):
        maps.append(mapsMaping[map])

  return maps

def checkForMultiMap(text):
    #Make sure tweet has a multiple maps across multiple sentences
    #Make sure tweet isn't a list
    # && !tweet.match(/[0-9]./)
    if(text.find(".") != -1):
        breakdown = text.split(".")
        #Check if Maps appear across in multiple lines
        count = 0
        for j in range(len(breakdown)):
            if(hasMapName(breakdown[j])):
                count+=1

        #If there are maps are mentioned across multiple maps, evaluate each line
        if(count > 1):
            return {
                "eval_seperate":True,
                "tweets": handleMultiMapTweet(text)
            }
        else:
            return {
                "eval_seperate":False,
                "tweets": text
            }
    return {
        "eval_seperate":False,
        "tweets": text
    }

def addToRedis(tweet, sentiment):
    url = ''
    myobj = {
        'tweet' : tweet[0],
        'username' : tweet[3],
        'at': tweet[11],
        'img': tweet[12],
        'id': tweet[10],
        'positive': sentiment,
        'incorrect': '',
    }
    x = requests.post(url, data = myobj)
    return x

def downloadCSV():
    today = date.today()
    d = today.strftime("%Y-%m-%d")
    filename = "tweets"+d+".csv"
    
    s3 = boto3.client('s3', aws_access_key_id='' , aws_secret_access_key='')
    s3.download_file('tweets-to-be-processed', filename, "./tweets.csv")

#Prep and Load Modal
tokenizer = BertTokenizer.from_pretrained(
    'bert-base-uncased',
    do_lower_case=True
)
model = BertForSequenceClassification.from_pretrained(
    'bert-base-uncased', 
    num_labels=6,
    output_attentions=False,
    output_hidden_states=False
)
device = torch.device('cude' if torch.cuda.is_available() else 'cpu')
model.to(device)
model.load_state_dict(
    torch.load(
        'Models/BERT_ft_epoch4.model',
        map_location=torch.device('cpu')
))
pipe = TextClassificationPipeline(model=model, tokenizer=tokenizer, return_all_scores=True)
sentimentMap = {"0" : "Happy", "1" : "Irrelevant", "2" : "Angry"}

#Get Today
today = date.today()
today = today.strftime("%Y-%m-%d")

#Get File
downloadCSV()
tweets = pd.read_csv('tweets.csv')
print(tweets)

#Connect to DB
hostname = ''
username = ''
password = ''
database = ''
conn = psycopg2.connect( host=hostname, user=username, password=password, dbname=database )
cur = conn.cursor()
count = 0
tweetsAdded = []
for tweet in tweets.iterrows():
    #Breakdown Tweet
    tweet = tweet[1]
    text = tweet["tweet"]
    tweet_id = tweet["tweet_id"]
    #map = tweet["map"]

    #Clean Text
    cleantext = cleanTweet(text)
    multimaps = multipleMapNames(text)

    #Get Full Tweet
    q = "SELECT * from public.tweets where id = '" + str(tweet_id) + "'"
    cur.execute(q)
    res = cur.fetchall()
    fulltweet = res[0]
    at = str(fulltweet[11])
    img = str(fulltweet[12])
    print("tweet:", count)
    count += 1

    res = checkForMultiMap(text)

    if(multimaps and res["eval_seperate"]):
        #Save sentiment anal for for each senence and each map in tweet
        breakdown = res["tweets"]
        for t in breakdown:
            cleantext = cleanTweet(t)
            maps = getMaps(t)
            sentiment = analyzeText(pipe, cleantext)
            for map in maps:
                q = "INSERT into public.checked_tweets (tweet, likes, username, profile, positive, map, id, date, at, img) VALUES ('" + text + "'," + str(fulltweet[1]) + ",'" + fulltweet[3] + "','" + str(fulltweet[4]) + "','" + str(sentiment) + "','" + str(map) + "','" + str(tweet_id) + "','" + today + "','" + at + "','" + img + "')"
                cur.execute(q)
                if(str(sentiment) != "1" and tweet_id not in tweetsAdded):
                    print("Added to Cache")
                    print(addToRedis(fulltweet, sentiment))
                    tweetsAdded.append(tweet_id)
    else:
        maps = getMaps(text)
        sentiment = analyzeText(pipe, cleantext)
        for map in maps:
            q = "INSERT into public.checked_tweets (tweet, likes, username, profile, positive, map, id, date, at, img) VALUES ('" + text + "'," + str(fulltweet[1]) + ",'" + fulltweet[3] + "','" + str(fulltweet[4]) + "','" + str(sentiment) + "','" + str(map) + "','" + str(tweet_id) + "','" + today + "','" + at + "','" + img + "')"
            cur.execute(q)
            if(str(sentiment) != "1" and tweet_id not in tweetsAdded):
                    print("Added to Cache")
                    addToRedis(fulltweet, sentiment)
                    tweetsAdded.append(tweet_id)

conn.commit()
cur.close()
conn.close()