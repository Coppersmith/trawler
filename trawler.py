#!/usr/bin/env python
"""
This script downloads Tweets for a given list of usernames and their FF networks to a specified depth.
Instantiate with -h option to view help info.
"""

# Standard Library modules
import argparse
import codecs
import os
import sys
import yaml
import datetime
import collections

import pprint

# Third party modules
from twython import Twython, TwythonError

# Local modules
from twitter_crawler import (CrawlTwitterTimelines, RateLimitedTwitterEndpoint, FindFriendFollowers,
                             get_console_info_logger, get_screen_names_from_file, save_tweets_to_json_file,
                             save_screen_names_to_file)

ff_scanned_screen_names = [] # global to avoid geting ff for same person twice
def get_ff(screen_names, depth, ff_finder, logger):
    if depth == 0:
        return screen_names
    else:
        next_level_sns = []
        for screen_name in screen_names:
            if screen_name not in ff_scanned_screen_names: # don't get ff for same person twice
                print "Print! Getting ff for %s" % screen_name
                logger.info("Getting ff for %s" % screen_name)
                ff_scanned_screen_names.append(screen_name)
                next_level_sns += ff_finder.get_ff_screen_names_for_screen_name(screen_name)
        return get_ff(screen_names + next_level_sns, depth-1, ff_finder, logger) # recursion

def main():
    # Make stdout output UTF-8, preventing "'ascii' codec can't encode" errors
    sys.stdout = codecs.getwriter('utf8')(sys.stdout)

    # Parse and document command line options
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('-sn', dest='screen_name_file', default="example_screen_names.txt",
                   help='A text file with one screen name per line.')
    parser.add_argument('-t', dest='token_file', default=os.path.expanduser("~") + "/.trawler/default.yaml",
                    help='A configuration file with Twitter API access tokens. See example_token_file.yaml.')
    parser.add_argument('-d', dest='depth', default=0,
                    help='Friend and follower depth. A value of 1 will gather all tweets for users \
                    in the file as well as all tweets from their friends and followers. Default is 0.')
    args = parser.parse_args()

    # Set up loggers and output directory
    logger = get_console_info_logger()
    output_directory = "data/" + datetime.datetime.now().isoformat() + "/"
    try:
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
    except:
        print "Could not create directory:", directory
        exit(0)
    logger.info("Created directory: %s" % output_directory)

    # Set up API access
    tokens = yaml.safe_load(open(args.token_file))
    ACCESS_TOKEN = Twython(tokens['consumer_key'], tokens['consumer_secret'], oauth_version=2).obtain_access_token()
    twython = Twython(tokens['consumer_key'], access_token=ACCESS_TOKEN)
    crawler = RateLimitedTwitterEndpoint(twython, "statuses/user_timeline", logger)

    # Gather unique screen names
    screen_names = get_screen_names_from_file(args.screen_name_file)
    depth = int(args.depth) # todo, validate args.depth
    unique_screen_names = []
    if depth > 0: # don't initiate ff_finder unless we have to
        ff_finder = FindFriendFollowers(twython, logger)
        ff_screen_names = get_ff(screen_names, depth, ff_finder, logger)
        unique_screen_names = set(ff_screen_names)
    else:
        unique_screen_names = set(screen_names) # assume the list has redundant names
    save_screen_names_to_file(unique_screen_names, output_directory + 'screen_names')

    # Gather tweets for each of the unique screen names
    for screen_name in unique_screen_names:
        tweet_filename = output_directory + screen_name + ".tweets"
        if os.path.exists(tweet_filename):
            logger.info("File '%s' already exists - will not attempt to download Tweets for '%s'" % (tweet_filename, screen_name))
        else:
            try:
                logger.info("Retrieving Tweets for user " + screen_name + " writing to file " + tweet_filename)
                tweets = crawler.get_data(screen_name=screen_name, count=200)
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
                save_tweets_to_json_file(tweets, tweet_filename)


if __name__ == "__main__":
    main()
