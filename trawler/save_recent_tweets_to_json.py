#!/usr/bin/env python

"""
This script downloads all "new" Tweets for a list of usernames that
have been posted since the last time the users' feeds were crawled.

The script takes as input:
  - a text file which lists one Twitter username per line of the file
  - the path to the existing [username].tweets files
  - the path where the new [username].tweets files will be stored

For each username, the script opens the '[old_path]/[username].tweets'
file, determines the most recently downloaded Tweet, and then creates
a new '[new_path]/[username].tweets' file containing any new Tweets
from the user.

Your Twitter OAuth credentials should be stored in the file
twitter_oauth_settings.py.
"""

# Standard Library modules
import argparse
import codecs
import json
import os
import sys

# Third party modules
from twython import Twython, TwythonError

# Local modules
from twitter_crawler import (CrawlTwitterTimelines, RateLimitedTwitterEndpoint,
                             get_console_info_logger, get_screen_names_from_file, save_tweets_to_json_file)
try:
    from twitter_oauth_settings import access_token, access_token_secret, consumer_key, consumer_secret
except ImportError:
    print "You must create a 'twitter_oauth_settings.py' file with your Twitter API credentials."
    print "Please copy over the sample configuration file:"
    print "  cp twitter_oauth_settings.sample.py twitter_oauth_settings.py"
    print "and add your API credentials to the file."
    sys.exit()


def main():
    # Make stdout output UTF-8, preventing "'ascii' codec can't encode" errors
    sys.stdout = codecs.getwriter('utf8')(sys.stdout)

    parser = argparse.ArgumentParser(description="")
    parser.add_argument('screen_name_file')
    parser.add_argument('old_tweet_path')
    parser.add_argument('new_tweet_path')
    args = parser.parse_args()

    logger = get_console_info_logger()

    ACCESS_TOKEN = Twython(consumer_key, consumer_secret, oauth_version=2).obtain_access_token()
    twython = Twython(consumer_key, access_token=ACCESS_TOKEN)

    crawler = CrawlTwitterTimelines(twython, logger)

    screen_names = get_screen_names_from_file(args.screen_name_file)

    for screen_name in screen_names:
        old_tweet_filename = os.path.join(args.old_tweet_path, "%s.tweets" % screen_name)
        new_tweet_filename = os.path.join(args.new_tweet_path, "%s.tweets" % screen_name)

        if not os.path.exists(old_tweet_filename):
            logger.error("Older Tweet file '%s' does not exist - will not attempt to download Tweets for '%s'" % (old_tweet_filename, screen_name))
            continue
        if os.path.exists(new_tweet_filename):
            logger.info("File '%s' already exists - will not attempt to download Tweets for '%s'" % (new_tweet_filename, screen_name))
            continue

        most_recent_tweet_id = get_most_recent_tweet_id_from_json_tweet_file(old_tweet_filename)

        try:
            tweets = crawler.get_all_timeline_tweets_for_screen_name_since(screen_name, most_recent_tweet_id)
        except TwythonError as e:
            print "TwythonError: %s" % e
            if e.error_code == 404:
                logger.warn("HTTP 404 error - Most likely, Twitter user '%s' no longer exists" % screen_name)
            elif e.error_code == 401:
                logger.warn("HTTP 401 error - Most likely, Twitter user '%s' no longer publicly accessible" % screen_name)
            else:
                # Unhandled exception
                raise e
        else:
            save_tweets_to_json_file(tweets, new_tweet_filename)


def get_most_recent_tweet_id_from_json_tweet_file(json_tweet_filename):
    """
    Assumes that Tweets in file are ordered newest to oldest
    """
    json_tweet_file = codecs.open(json_tweet_filename, "r", encoding="utf-8")
    first_tweet_json = json_tweet_file.readline()
    first_tweet = json.loads(first_tweet_json)
    most_recent_tweet_id = first_tweet['id']
    json_tweet_file.close()
    return most_recent_tweet_id


if __name__ == "__main__":
    main()
