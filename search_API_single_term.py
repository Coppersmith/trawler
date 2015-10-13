from twitter_crawler import get_search_crawler, get_connection
import yaml,gzip
from twython import Twython, TwythonError

search_term = 'qntfy'

import ujson as json

token_file = 'default_tokens.yaml'
tokens = yaml.safe_load(open(token_file))


# Set up API access
twython = Twython(app_key=tokens['app_key'], 
                  app_secret=tokens['app_secret_key'],
                  oauth_token=tokens['oauth_token'],
                  oauth_token_secret=tokens['oauth_token_secret'])

search_crawler = get_search_crawler( twython )


#Example uses here:
#tweets = search_crawler.get_all_search_tweets_for_term( search_term )
#tweets = search_crawler.get_all_search_tweets_for_term( search_term, max_id=653333775389229055 )#id of the latest tweet -1
#tweets = search_crawler.get_all_search_tweets_for_term( search_term, result_type='recent' ) #Other options include 'mixed'(default) and 'popular'
tweets = search_crawler.get_all_search_tweets_for_term( search_term, max_id=653333775389229055, result_type='recent' )#id of the latest tweet -1

with gzip.open('%s.json.gz' % search_term,'w') as OUT:
    for t in tweets:
        OUT.write('%s\n' % json.dumps(t))


