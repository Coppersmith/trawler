from twitter_crawler import get_search_crawler, get_connection
import yaml
from twython import Twython, TwythonError
search_term = 'comingoutday'

import ujson as json

"""
app_tokens = json.load(open('quark_oauth_app_key.json'))
auth_tokens = json.load(open('qntfy_oauth2_tokens_twitter.json'))
"""

"""
HARDCODE
"""




# Set up API access
#tokens = yaml.safe_load(open(token_file))
#app_key=None, app_secret=None, oauth_token=None, oauth_token_secret=None, access_token=None,
twython = Twython(app_key=app_key, app_secret=app_secret_key,
                       oauth_token=oauth_token,
                       oauth_token_secret=oauth_token_secret)
#oauth_version=1)#, oauth_version=1).obtain_access_token()
#print ACCESS_TOKEN

#twython = Twython(app_key, access_token=ACCESS_TOKEN)

print "----------------------"

search_crawler = get_search_crawler( twython )

search_crawler.get_all_search_tweets_for_term( search_term )

with gzip.open('%s.json.gz' % term,'w') as OUT:
    for t in tweets:
        OUT.write('%s\n' % json.dump(tweet))


