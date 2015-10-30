#!/usr/bin/env python
"""
This script downloads Tweets for a given list of usernames.
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
from twitter_crawler import (get_connection, save_tweets_to_json_file,
                             get_screen_names_from_file, get_timeline_crawler)

def main():
    # Make stdout output UTF-8, preventing "'ascii' codec can't encode" errors
    sys.stdout = codecs.getwriter('utf8')(sys.stdout)

    # Parse and document command line options
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('--input', dest='screen_name_file', default="example_screen_names.txt",
                   help='A text file with one screen name per line.')
    parser.add_argument('--token', dest='token_file', default=os.path.expanduser("~") + "/.trawler/default.yaml",
                    help='A configuration file with Twitter API access tokens. See example_token_file.yaml or twitter_oauth_settings.sample.py')
    parser.add_argument('--output', dest='output', default='./',
                    help='Where to output the resulting data.')
    args = parser.parse_args()

    # Set up loggers and output directory
    logger = get_console_info_logger()
    output_directory = args.output
    try:
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
    except:
        print "Could not create directory:", directory
        exit(0)
    logger.info("Created directory: %s" % output_directory)

    # Set up API access
    if args.tokenfile.endswith('yaml'):
        #YAML file
        tokens = yaml.safe_load(open(args.token_file))
    elif args.tokenfile.endswith('py'):
        #.py file -- surely there is a better way to do this
        tokens = {}
        for line in open(args.token_file):
            k,v = [x.strip() for x in line.split("=")]
            tokens[k] = v[1:-1]
    else:
        raise "Unrecognized token file type -- please use a .yaml or .py file following the examples"
            
    ACCESS_TOKEN = Twython(tokens['consumer_key'], tokens['consumer_secret'], oauth_version=2).obtain_access_token()
    twython = Twython(tokens['consumer_key'], access_token=ACCESS_TOKEN)
    crawler = RateLimitedTwitterEndpoint(twython, "statuses/user_timeline", logger)

    # Gather unique screen names
    screen_names = get_screen_names_from_file(args.screen_name_file)

    # Gather tweets for each of the unique screen names
    # NB: in production, one should use `id` as an identifier (which does not change)
    # rather than the `screen_name`, which can be changed at the users's whim.
    for screen_name in unique_screen_names:
        tweet_filename = output_directory + screen_name + ".tweets.gz" 
        if os.path.exists(tweet_filename):
            logger.info("File '%s' already exists - will not attempt to download Tweets for '%s'" % (tweet_filename, screen_name))
        else:
            tweets = crawler.get_all_timeline_tweets_for_screen_name( screen_name )
            #Write them out as one-JSON-object-per-line in a gzipped file
            save_tweets_to_json_file(tweets, tweet_filename, gzip=True)


if __name__ == "__main__":
    main()
