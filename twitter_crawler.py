"""
Shared classes and functions for crawling Twitter
"""

# Standard Library modules
import codecs
import datetime
import itertools
import logging
import time
import gzip

try:
    import ujson as json #much quicker
except:
    import json

# Third party modules
from twython import Twython, TwythonError



###  Functions  ###

def get_console_info_logger():
    """
    Return a logger that logs INFO and above to stderr
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)
    return logger


def get_screen_names_from_file(filename):
    """
    Opens a text file containing one Twitter screen name per line,
    returns a list of the screen names.
    """
    screen_name_file = codecs.open(filename, "r", "utf-8")
    screen_names = []
    for line in screen_name_file.readlines():
        if line.strip():
            screen_names.append(line.strip())
    screen_name_file.close()
    return screen_names

def get_ids_from_file(filename):
    """
    Opens a text file containing one Twitter `user_id` per line,
    returns a list of the properly casted `user_id`s.
    """
    id_file = codecs.open(filename, "r", "utf-8")
    ids = []
    for line in id_file.readlines():
        stripped = line.strip()
        if stripped:
            ids.append(int(stripped))
    id_file.close()
    return ids


def grouper(iterable, n, fillvalue=None):
    """Collect data into fixed-length chunks or blocks"""
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    # Taken from: http://docs.python.org/2/library/itertools.html
    args = [iter(iterable)] * n
    return itertools.izip_longest(fillvalue=fillvalue, *args)


def save_screen_names_to_file(screen_names, filename, logger=None):
    """
    Saves a list of Twitter screen names to a text file with one
    screen name per line.
    """
    if logger:
        logger.info("Saving %d screen names to file '%s'" % (len(screen_names), filename))
    f = codecs.open(filename, 'w', 'utf-8')
    for screen_name in screen_names:
        f.write("%s\n" % screen_name)
    f.close()


def save_tweets_to_json_file(tweets, json_filename, gzip_out=False):
    """
    Takes a Python dictionary of Tweets from the Twython API, and
    saves the Tweets to a JSON file, storing one JSON object per
    line.
    `gzip_out=True` will write it to a gzip file, rather than a flat file
    """
    if gzip_out:
        OUT = gzip.open(json_filename, 'wb')
        for tweet in tweets:
            OUT.write(unicode("%s\n" % json.dumps(tweet)).encode('utf-8'))
    else:
        json_file = codecs.open(json_filename, "w", "utf-8")
        for tweet in tweets:
            json_file.write("%s\n" % json.dumps(tweet))
        json_file.close()

def tweets_to_kafka_stream(tweets, channel='trawler', kafka_producer=None,
                                host=None, port=None):
    """
    Sends each tweet in `tweets` as a message on the Kafka channel `channel`.
    Requires either a `kafka_producer`, an instance of `kafka.SimpleProducer` or
    the `host` and `port` upon which to connect.
    """
    if not kafka_producer:
        from kafka import KafkaClient, SimpleProducer
        kafka_client = KafkaClient('%s:%s' % (host,port) )
        kafka_producer = SimpleProducer(kafka_client)

    for tweet in tweets:
        producer.send_messages(channel, json.dums(tweet))


###  Classes  ###

class CrawlTwitterTimelines:
    def __init__(self, twython, logger=None):
        if logger is None:
            self._logger = get_console_info_logger()
        else:
            self._logger = logger

        self._twitter_endpoint = RateLimitedTwitterEndpoint(twython, "statuses/user_timeline", logger=self._logger)

###
### Accessing the users by `screen_name`
###


    def get_all_timeline_tweets_for_screen_name(self, screen_name):
        """
        Retrieves all Tweets from a user's timeline based on this procedure:
          https://dev.twitter.com/docs/working-with-timelines
        """
        # This function stops requesting additional Tweets from the timeline only
        # if the most recent number of Tweets retrieved is less than 100.
        #
        # This threshold may need to be adjusted.
        #
        # While we request 200 Tweets with each API, the number of Tweets we retrieve
        # will often be less than 200 because, for example, "suspended or deleted
        # content is removed after the count has been applied."  See the API
        # documentation for the 'count' parameter for more info:
        #   https://dev.twitter.com/docs/api/1.1/get/statuses/user_timeline
        MINIMUM_TWEETS_REQUIRED_FOR_MORE_API_CALLS = 100

        self._logger.info("Retrieving Tweets for user '%s'" % screen_name)

        # Retrieve first batch of Tweets
        tweets = self._twitter_endpoint.get_data(screen_name=screen_name, count=200)
        self._logger.info("  Retrieved first %d Tweets for user '%s'" % (len(tweets), screen_name))

        if len(tweets) < MINIMUM_TWEETS_REQUIRED_FOR_MORE_API_CALLS:
            return tweets

        # Retrieve rest of Tweets
        while 1:
            max_id = int(tweets[-1]['id']) - 1
            more_tweets = self._twitter_endpoint.get_data(screen_name=screen_name, count=200, max_id=max_id)
            tweets += more_tweets
            self._logger.info("  Retrieved %d Tweets for user '%s' with max_id='%d'" % (len(more_tweets), screen_name, max_id))

            if len(more_tweets) < MINIMUM_TWEETS_REQUIRED_FOR_MORE_API_CALLS:
                return tweets

    def get_most_recent_tweets(self, screen_name):
        """
        Makes a single call to the user's timeline to obtain the most recent
        (up to 200) tweets. Great for getting a better snapshot of user behavior
        than available from a single tweet.
        """
        self._logger.info("Retrieving Tweets for user '%s'" % screen_name)

        tweets = self._twitter_endpoint.get_data(screen_name=screen_name,count=200)
        self._logger.info("  Retrieved first %d Tweets for user '%s'" % (len(tweets),screen_name))
        return tweets

    def get_all_timeline_tweets_for_screen_name_since(self, screen_name, since_id,max_id=None):
        """
        Retrieves all Tweets from a user's timeline since the specified Tweet ID
        based on this procedure:
          https://dev.twitter.com/docs/working-with-timelines
        """
        # This function stops requesting additional Tweets from the timeline only
        # if the most recent number of Tweets retrieved is less than 100.
        #
        # This threshold may need to be adjusted.
        #
        # While we request 200 Tweets with each API, the number of Tweets we retrieve
        # will often be less than 200 because, for example, "suspended or deleted
        # content is removed after the count has been applied."  See the API
        # documentation for the 'count' parameter for more info:
        #   https://dev.twitter.com/docs/api/1.1/get/statuses/user_timeline
        MINIMUM_TWEETS_REQUIRED_FOR_MORE_API_CALLS = 100

        self._logger.info("Retrieving Tweets for user '%s'" % screen_name)

        # Retrieve first batch of Tweets
        if not max_id:
            tweets = self._twitter_endpoint.get_data(screen_name=screen_name, count=200, since_id=since_id)
            self._logger.info("  Retrieved first %d Tweets for user '%s'" % (len(tweets), screen_name))

            if len(tweets) < MINIMUM_TWEETS_REQUIRED_FOR_MORE_API_CALLS:
                return tweets
        else:
            tweets = []

        # Retrieve rest of Tweets
        while 1:
            if tweets: #Will only trigger
                max_id = int(tweets[-1]['id']) - 1
            more_tweets = self._twitter_endpoint.get_data(screen_name=screen_name, count=200, max_id=max_id, since_id=since_id)
            tweets += more_tweets
            self._logger.info("  Retrieved %d Tweets for user '%s' with max_id='%d'" % (len(more_tweets), screen_name, since_id))

            if len(more_tweets) < MINIMUM_TWEETS_REQUIRED_FOR_MORE_API_CALLS:
                return tweets


###
### Accessing the users by `user_id`
###


    def get_all_timeline_tweets_for_id(self, user_id):
        """
        Retrieves all Tweets from a user's timeline based on this procedure:
          https://dev.twitter.com/docs/working-with-timelines
        """
        # This function stops requesting additional Tweets from the timeline only
        # if the most recent number of Tweets retrieved is less than 100.
        #
        # While we request 200 Tweets with each API, the number of Tweets we retrieve
        # will often be less than 200 because, for example, "suspended or deleted
        # content is removed after the count has been applied."  See the API
        # documentation for the 'count' parameter for more info:
        #   https://dev.twitter.com/docs/api/1.1/get/statuses/user_timeline
        MINIMUM_TWEETS_REQUIRED_FOR_MORE_API_CALLS = 100

        self._logger.info("Retrieving Tweets for user_id '%s'" % user_id)

        # Retrieve first batch of Tweets
        tweets = self._twitter_endpoint.get_data(user_id=user_id, count=200)
        self._logger.info("  Retrieved first %d Tweets for user_id '%s'" % (len(tweets), user_id))

        if len(tweets) < MINIMUM_TWEETS_REQUIRED_FOR_MORE_API_CALLS:
            return tweets

        # Retrieve rest of Tweets
        while 1:
            max_id = int(tweets[-1]['id']) - 1
            more_tweets = self._twitter_endpoint.get_data(user_id=user_id, count=200, max_id=max_id)
            tweets += more_tweets
            self._logger.info("  Retrieved %d Tweets for user '%s' with max_id='%d'" % (len(more_tweets), user_id, max_id))

            if len(more_tweets) < MINIMUM_TWEETS_REQUIRED_FOR_MORE_API_CALLS:
                return tweets

    def get_most_recent_tweets_by_id(self, user_id):
        """
        Makes a single call to the user's timeline to obtain the most recent
        (up to 200) tweets. Great for getting a better snapshot of user behavior
        than available from a single tweet.
        """
        self._logger.info("Retrieving Tweets for user_id '%s'" % user_id)

        tweets = self._twitter_endpoint.get_data(user_id=user_id,count=200)
        self._logger.info("  Retrieved first %d Tweets for user '%s'" % (len(tweets),user_id))
        return tweets

    def get_all_timeline_tweets_for_id_since(self, user_id, since_id,max_id=None):
        """
        Retrieves all Tweets from a user's timeline since the specified Tweet ID
        based on this procedure:
          https://dev.twitter.com/docs/working-with-timelines
        """
        # This function stops requesting additional Tweets from the timeline only
        # if the most recent number of Tweets retrieved is less than 100.
        #
        # While we request 200 Tweets with each API, the number of Tweets we retrieve
        # will often be less than 200 because, for example, "suspended or deleted
        # content is removed after the count has been applied."  See the API
        # documentation for the 'count' parameter for more info:
        #   https://dev.twitter.com/docs/api/1.1/get/statuses/user_timeline
        MINIMUM_TWEETS_REQUIRED_FOR_MORE_API_CALLS = 100

        self._logger.info("Retrieving Tweets for user_id '%s'" % user_id)

        # Retrieve first batch of Tweets
        if not max_id:
            tweets = self._twitter_endpoint.get_data(user_id=user_id, count=200, since_id=since_id)
            self._logger.info("  Retrieved first %d Tweets for user '%s'" % (len(tweets), user_id))

            if len(tweets) < MINIMUM_TWEETS_REQUIRED_FOR_MORE_API_CALLS:
                return tweets
        else:
            tweets = []

        # Retrieve rest of Tweets
        while 1:
            if tweets: #Will only trigger
                max_id = int(tweets[-1]['id']) - 1
            more_tweets = self._twitter_endpoint.get_data(user_id=user_id, count=200, max_id=max_id, since_id=since_id)
            tweets += more_tweets
            self._logger.info("  Retrieved %d Tweets for user_id '%s' with max_id='%d'" % (len(more_tweets), user_id, since_id))

            if len(more_tweets) < MINIMUM_TWEETS_REQUIRED_FOR_MORE_API_CALLS:
                return tweets

    def get_all_timeline_tweets_for_id_between_ids(self, user_id, since_id, max_id):
        """
        Retrieves all Tweets from a user's timeline since the specified Tweet ID
        based on this procedure:
          https://dev.twitter.com/docs/working-with-timelines
        """
        # This function stops requesting additional Tweets from the timeline only
        # if the most recent number of Tweets retrieved is less than 100.
        #
        # This threshold may need to be adjusted.
        #
        # While we request 200 Tweets with each API, the number of Tweets we retrieve
        # will often be less than 200 because, for example, "suspended or deleted
        # content is removed after the count has been applied."  See the API
        # documentation for the 'count' parameter for more info:
        #   https://dev.twitter.com/docs/api/1.1/get/statuses/user_timeline
        MINIMUM_TWEETS_REQUIRED_FOR_MORE_API_CALLS = 100

        self._logger.info("Retrieving Tweets for user '%s'" % user_id)

        # Retrieve first batch of Tweets
        tweets = self._twitter_endpoint.get_data(user_id=user_id, count=200, since_id=since_id, max_id=max_id)
        self._logger.info("  Retrieved first %d Tweets for user '%s'" % (len(tweets), user_id))

        if len(tweets) < MINIMUM_TWEETS_REQUIRED_FOR_MORE_API_CALLS:
            return tweets

        # Retrieve rest of Tweets
        while 1:
            max_id = int(tweets[-1]['id']) - 1 #It's okay that this adjusts the max_id, since we are going backwards in time
            more_tweets = self._twitter_endpoint.get_data(user_id=user_id, count=200, max_id=max_id, since_id=since_id)
            tweets += more_tweets
            self._logger.info("  Retrieved %d Tweets for user '%s' with max_id='%d'" % (len(more_tweets), user_id, since_id))

            if len(more_tweets) < MINIMUM_TWEETS_REQUIRED_FOR_MORE_API_CALLS:
                return tweets



import datetime as dt
class FindFriendFollowers:
    def __init__(self, twython, logger=None):
        if logger is None:
            self._logger = get_console_info_logger()
        else:
            self._logger = logger

        self._friend_endpoint = RateLimitedTwitterEndpoint(twython, "friends/ids", logger=self._logger)
        self._follower_endpoint = RateLimitedTwitterEndpoint(twython, "followers/ids", logger=self._logger)
        self._user_lookup_endpoint = RateLimitedTwitterEndpoint(twython, "users/lookup", logger=self._logger)
        self.calls_remaining = 1
        self.last_checked_status = dt.datetime.now()

    def api_calls_remaining(self):
        now = dt.datetime.now()
        #If it's been more than 7 minutes, go check the status before blindly returning
        if (now - self.last_checked_status) > dt.timedelta(minutes=7):
            self.last_checked_status = dt.datetime.now()
            self._friend_endpoint.update_rate_limit_status()
            self._follower_endpoint.update_rate_limit_status()
            self._user_lookup_endpoint.update_rate_limit_status()
        return min(self._friend_endpoint.api_calls_remaining_for_current_window,
                   self._follower_endpoint.api_calls_remaining_for_current_window,
                   self._user_lookup_endpoint.api_calls_remaining_for_current_window)


###
### Accessing data by screen_name
###

    def get_ff_ids_for_screen_name(self, screen_name):
        """
        Returns Twitter user IDs for users who are both Friends and Followers
        for the specified screen_name.

        The 'friends/ids' and 'followers/ids' endpoints return at most 5000 IDs,
        so IF a user has more than 5000 friends or followers, this function WILL
        NOT RETURN THE CORRECT ANSWER
        """
        try:
            friend_ids = self._friend_endpoint.get_data(screen_name=screen_name)[u'ids']
            follower_ids = self._follower_endpoint.get_data(screen_name=screen_name)[u'ids']
        except TwythonError as e:
            if e.error_code == 404:
                self._logger.warn("HTTP 404 error - Most likely, Twitter user '%s' no longer exists" % screen_name)
            elif e.error_code == 401:
                self._logger.warn("HTTP 401 error - Most likely, Twitter user '%s' no longer publicly accessible" % screen_name)
            else:
                # Unhandled exception
                raise e
            friend_ids = []
            follower_ids = []

        return list(set(friend_ids).intersection(set(follower_ids)))


    def get_ff_screen_names_for_screen_name(self, screen_name):
        """
        Returns Twitter screen names for users who are both Friends and Followers
        for the specified screen_name.
        """
        ff_ids = self.get_ff_ids_for_screen_name(screen_name)

        ff_screen_names = []
        # The Twitter API allows us to look up info for 100 users at a time
        for ff_id_subset in grouper(ff_ids, 100):
            user_ids = ','.join([str(id) for id in ff_id_subset if id is not None])
            users = self._user_lookup_endpoint.get_data(user_id=user_ids, entities=False)
            for user in users:
                ff_screen_names.append(user[u'screen_name'])
        return ff_screen_names

###
### Accessing data by user_id
###

    def get_ff_ids_for_id(self, user_id):
        """
        Returns Twitter user IDs for users who are both Friends and Followers
        for the specified `user_id`.

        The 'friends/ids' and 'followers/ids' endpoints return at most 5000 IDs,
        so IF a user has more than 5000 friends or followers, this function WILL
        NOT RETURN THE CORRECT ANSWER
        """
        try:
            friend_ids = self._friend_endpoint.get_data(user_id=user_id)[u'ids']
            follower_ids = self._follower_endpoint.get_data(user_id=user_id)[u'ids']
        except TwythonError as e:
            if e.error_code == 404:
                self._logger.warn("HTTP 404 error - Most likely, Twitter user '%s' no longer exists" % user_id)
            elif e.error_code == 401:
                self._logger.warn("HTTP 401 error - Most likely, Twitter user '%s' no longer publicly accessible" % user_id)
            else:
                # Unhandled exception
                raise e
            friend_ids = []
            follower_ids = []

        return list(set(friend_ids).intersection(set(follower_ids)))


    def get_ff_screen_names_for_id(self, user_id):
        """
        Returns Twitter screen names for users who are both Friends and Followers
        for the specified `user_id`.
        """
        ff_ids = self.get_ff_ids_for_screen_name(screen_name)

        ff_screen_names = []
        # The Twitter API allows us to look up info for 100 users at a time
        for ff_id_subset in grouper(ff_ids, 100):
            user_ids = ','.join([str(id) for id in ff_id_subset if id is not None])
            users = self._user_lookup_endpoint.get_data(user_id=user_ids, entities=False)
            for user in users:
                ff_screen_names.append(user[u'screen_name'])
        return ff_screen_names


class FindFollowers:
    def __init__(self, twython, logger=None):
        if logger is None:
            self._logger = get_console_info_logger()
        else:
            self._logger = logger

        self._follower_endpoint = RateLimitedTwitterEndpoint(twython, "followers/ids", logger=self._logger)
        self._user_lookup_endpoint = RateLimitedTwitterEndpoint(twython, "users/lookup", logger=self._logger)
        self.calls_remaining = 1
        self.last_checked_status = dt.datetime.now()

    def api_calls_remaining(self):
        now = dt.datetime.now()
        #If it's been more than 7 minutes, go check the status before blindly returning
        if (now - self.last_checked_status) > dt.timedelta(minutes=7):
            self.last_checked_status = dt.datetime.now()
            self._follower_endpoint.update_rate_limit_status()
            self._user_lookup_endpoint.update_rate_limit_status()
        return min(self._follower_endpoint.api_calls_remaining_for_current_window,
                   self._user_lookup_endpoint.api_calls_remaining_for_current_window)

###
### Access Users by `screen_name`
###

    def get_follower_ids_for_screen_name(self, screen_name):
        """
        Returns Twitter user IDs for users who are Followers of
        the specified screen_name.

        The 'followers/ids' endpoint return at most 5000 IDs,
        so IF a user has more than 5000 followers, this function WILL
        NOT RETURN THE CORRECT ANSWER
        """
        try:
            follower_ids = self._follower_endpoint.get_data(screen_name=screen_name)[u'ids']
        except TwythonError as e:
            if e.error_code == 404:
                self._logger.warn("HTTP 404 error - Most likely, Twitter user '%s' no longer exists" % screen_name)
            elif e.error_code == 401:
                self._logger.warn("HTTP 401 error - Most likely, Twitter user '%s' no longer publicly accessible" % screen_name)
            else:
                # Unhandled exception
                raise e
            follower_ids = []

        return follower_ids


    def get_follower_screen_names_for_screen_name(self, screen_name):
        """
        Returns Twitter screen names for users who are Followers
        of the specified screen_name.
        """
        follower_ids = self.get_follower_ids_for_screen_name(screen_name)

        follower_screen_names = []
        # The Twitter API allows us to look up info for 100 users at a time
        for follower_id_subset in grouper(follower_ids, 100):
            user_ids = ','.join([str(id) for id in follower_id_subset if id is not None])
            users = self._user_lookup_endpoint.get_data(user_id=user_ids, entities=False)
            for user in users:
                follower_screen_names.append(user[u'screen_name'])
        return follower_screen_names

###
### Access Users by `user_id`
###

    def get_follower_ids_for_id(self, user_id):
        """
        Returns Twitter user IDs for users who are Followers of
        the specified user_id.

        The 'followers/ids' endpoint return at most 5000 IDs,
        so IF a user has more than 5000 followers, this function WILL
        NOT RETURN THE CORRECT ANSWER
        """
        try:
            follower_ids = self._follower_endpoint.get_data(user_id=user_id)[u'ids']
        except TwythonError as e:
            if e.error_code == 404:
                self._logger.warn("HTTP 404 error - Most likely, Twitter user_id '%s' no longer exists" % user_id)
            elif e.error_code == 401:
                self._logger.warn("HTTP 401 error - Most likely, Twitter user_id '%s' no longer publicly accessible" % user_id)
            else:
                # Unhandled exception
                raise e
            follower_ids = []

        return follower_ids


    def get_follower_screen_names_for_id(self, user_id):
        """
        Returns Twitter screen names for users who are Followers
        of the specified `user_id`.
        """
        follower_ids = self.get_follower_ids_for_screen_name(user_id)

        follower_screen_names = []
        # The Twitter API allows us to look up info for 100 users at a time
        for follower_id_subset in grouper(follower_ids, 100):
            user_ids = ','.join([str(id) for id in follower_id_subset if id is not None])
            users = self._user_lookup_endpoint.get_data(user_id=user_ids, entities=False)
            for user in users:
                follower_screen_names.append(user[u'screen_name'])
        return follower_screen_names


class FindFollowees:
    def __init__(self, twython, logger=None):
        if logger is None:
            self._logger = get_console_info_logger()
        else:
            self._logger = logger

        self._followee_endpoint = RateLimitedTwitterEndpoint(twython, "followers/ids", logger=self._logger)
        self._user_lookup_endpoint = RateLimitedTwitterEndpoint(twython, "users/lookup", logger=self._logger)
        self.calls_remaining = 1
        self.last_checked_status = dt.datetime.now()

    def api_calls_remaining(self):
        now = dt.datetime.now()
        #If it's been more than 7 minutes, go check the status before blindly returning
        if (now - self.last_checked_status) > dt.timedelta(minutes=7):
            self.last_checked_status = dt.datetime.now()
            self._followee_endpoint.update_rate_limit_status()
            self._user_lookup_endpoint.update_rate_limit_status()
        return min(self._followee_endpoint.api_calls_remaining_for_current_window,
                   self._user_lookup_endpoint.api_calls_remaining_for_current_window)

###
### Access Users by `screen_name`
###

    def get_followee_ids_for_screen_name(self, screen_name):
        """
        Returns Twitter user IDs for users who `screen_name` follows.

        The 'followers/ids' endpoint return at most 5000 IDs,
        so IF a user has more than 5000 followers, this function WILL
        NOT RETURN THE CORRECT ANSWER
        """
        try:
            followee_ids = self._followee_endpoint.get_data(screen_name=screen_name)[u'ids']
        except TwythonError as e:
            if e.error_code == 404:
                self._logger.warn("HTTP 404 error - Most likely, Twitter user '%s' no longer exists" % screen_name)
            elif e.error_code == 401:
                self._logger.warn("HTTP 401 error - Most likely, Twitter user '%s' no longer publicly accessible" % screen_name)
            else:
                # Unhandled exception
                raise e
            followee_ids = []

        return followee_ids


    def get_followee_screen_names_for_screen_name(self, screen_name):
        """
        Returns Twitter screen_names for users who `screen_name` follows.
        """
        followee_ids = self.get_followee_ids_for_screen_name(screen_name)

        followee_screen_names = []
        # The Twitter API allows us to look up info for 100 users at a time
        for followee_id_subset in grouper(followee_ids, 100):
            user_ids = ','.join([str(id) for id in followee_id_subset if id is not None])
            users = self._user_lookup_endpoint.get_data(user_id=user_ids, entities=False)
            for user in users:
                followee_screen_names.append(user[u'screen_name'])
        return followee_screen_names

###
### Access Users by `user_id`
###

    def get_followee_ids_for_id(self, user_id):
        """
        Returns Twitter user IDs for users who `user_id` follows.

        The 'followers/ids' endpoint return at most 5000 IDs,
        so IF a user has more than 5000 followers, this function WILL
        NOT RETURN THE CORRECT ANSWER
        """
        try:
            followee_ids = self._followee_endpoint.get_data(user_id=user_id)[u'ids']
        except TwythonError as e:
            if e.error_code == 404:
                self._logger.warn("HTTP 404 error - Most likely, Twitter user_id '%s' no longer exists" % user_id)
            elif e.error_code == 401:
                self._logger.warn("HTTP 401 error - Most likely, Twitter user_id '%s' no longer publicly accessible" % user_id)
            else:
                # Unhandled exception
                raise e
            followee_ids = []

        return followee_ids


    def get_followee_screen_names_for_id(self, user_id):
        """
        Returns Twitter screen names for users who `user_id` follows.
        """
        followee_ids = self.get_followee_ids_for_screen_name(user_id)

        followee_screen_names = []
        # The Twitter API allows us to look up info for 100 users at a time
        for followee_id_subset in grouper(followee_ids, 100):
            user_ids = ','.join([str(id) for id in followee_id_subset if id is not None])
            users = self._user_lookup_endpoint.get_data(user_id=user_ids, entities=False)
            for user in users:
                followee_screen_names.append(user[u'screen_name'])
        return followee_screen_names

class UserLookup:
    def __init__(self, twython, logger=None):
        if logger is None:
            self._logger = get_console_info_logger()
        else:
            self._logger = logger

        self._user_lookup_endpoint = RateLimitedTwitterEndpoint(twython, "users/lookup", logger=self._logger)
        self.calls_remaining = 1
        self.last_checked_status = dt.datetime.now()

    def api_calls_remaining(self):
        now = dt.datetime.now()
        #If it's been more than 7 minutes, go check the status before blindly returning
        if (now - self.last_checked_status) > dt.timedelta(minutes=7):
            self.last_checked_status = dt.datetime.now()
            self._user_lookup_endpoint.update_rate_limit_status()
        return min(self._user_lookup_endpoint.api_calls_remaining_for_current_window)

    def lookup_users(self, twitter_ids):
        """
        Returns the user lookup for the users specified by `twitter_id`
        To maximize throughput of Twitter API, this looks up 100 users with a
        single call.
        """
        # The Twitter API allows us to look up info for 100 users at a time
        amassed_users = []
        for id_subset in grouper(twitter_ids, 100):
            user_ids = ','.join([str(id) for id in id_subset if id is not None])
            users = self._user_lookup_endpoint.get_data(user_id=user_ids, entities=False)
            amassed_users += users
        return amassed_users

class ListMembership:
    def __init__(self, twython, logger=None):
        if logger is None:
            self._logger = get_console_info_logger()
        else:
            self._logger = logger

        self._lists_memberships_endpoint = RateLimitedTwitterEndpoint(twython, "lists/memberships", logger=self._logger)
        self._lists_members_endpoint = RateLimitedTwitterEndpoint(twython, "lists/members", logger=self._logger)
        self._user_lookup_endpoint = RateLimitedTwitterEndpoint(twython, "users/lookup", logger=self._logger)
        self.calls_remaining = 1
        self.last_checked_status = dt.datetime.now()

    def api_calls_remaining(self):
        now = dt.datetime.now()
        #If it's been more than 7 minutes, go check the status before blindly returning
        if (now - self.last_checked_status) > dt.timedelta(minutes=7):
            self.last_checked_status = dt.datetime.now()
            self._lists_memberships_endpoint.update_rate_limit_status()
            self._lists_members_endpoint.update_rate_limit_status()
            self._user_lookup_endpoint.update_rate_limit_status()
        return min(self._lists_memberships_endpoint.api_calls_remaining_for_current_window,
                   self._lists_members_endpoint.api_calls_remaining_for_current_window,
                   self._user_lookup_endpoint.api_calls_remaining_for_current_window)


###
### Accessing data by screen_name
###

    def get_list_memberships_for_screen_name(self, screen_name):
        """
        Returns full listings for any lists this `screen_name` was added to.

        Unclear what will happen if `screen_name` is a member of many many lists.
        """
        try:
            list_memberships = self._lists_memberships_endpoint.get_data(screen_name=screen_name)[u'lists']
            #follower_ids = self._follower_endpoint.get_data(screen_name=screen_name)[u'ids']
        except TwythonError as e:
            print "Error:", e.error_code
            print e
            raise e
            """
            if e.error_code == 404:
                self._logger.warn("HTTP 404 error - Most likely, Twitter user '%s' no longer exists" % screen_name)
            elif e.error_code == 401:
                self._logger.warn("HTTP 401 error - Most likely, Twitter user '%s' no longer publicly accessible" % screen_name)
            else:
                # Unhandled exception
                raise e
            """
            list_memberships = []
        return list_memberships

    def get_list_membership_ids_for_screen_name( self, screen_name):
        """
        Returns the list `id`s for each list that `screen_name` is a member of.
        """
        return [x['id'] for x in self.get_list_memberships_for_screen_name(screen_name)]

    def get_list_members_by_list_id( self, list_id):
        """
        Returns the users who are members of list `list_id`.
        This output is cursored, which is not currently implemented, so only the
        first 5000 members of each list are returned.
        TODO: add cursor paging functionality.
        """
        # Retrieve first batch of members
        response = self._lists_members_endpoint.get_data(list_id=list_id, count=5000 )
        members = response['users']
        self._logger.info("  Retrieved first %d Members for List '%s'" % (len(members), list_id))

        return members



class RateLimitedTwitterEndpoint:
    """
    Class used to retrieve data from a Twitter API endpoint without
    violating Twitter's API rate limits for that API endpoint.

    Each Twitter API endpoint (e.g. 'statuses/user_timeline') has its
    own number of allotted requests per rate limit duration window:

      https://dev.twitter.com/docs/rate-limiting/1.1/limits

    The RateLimitedTwitterEndpoint class has a single public function,
    get_data(), that is a thin wrapper around the Twitter API.  If the
    rate limit for the current window has been reached, the get_data()
    function will block for up to 15 minutes until the next rate limit
    window starts.

    Only one RateLimitedTwitterEndpoint instance should be running
    anywhere in the world per (Twitter API key, Twitter API endpoint)
    pair.  Each class instance assumes it is the only program using up
    the API calls available for the current rate limit window.
    """
    def __init__(self, twython, twitter_api_endpoint, logger=None):
        """
        twython -- an instance of a twython.Twython object that has
        been initialized with a valid set of Twitter API credentials.

        twitter_api_endpoint -- a string that names a Twitter API
        endpoint (e.g. 'followers/ids', 'statuses/mentions_timeline').
        The endpoint string should NOT have a leading slash (use
        'followers/ids', NOT '/followers/ids').  For a full list of
        endpoints, see:

          https://dev.twitter.com/docs/api/1.1

        logger -- an optional instance of a logging.Logger class.
        """
        self._twython = twython
        self._twitter_api_endpoint = twitter_api_endpoint
        self._twitter_api_endpoint_with_prefix = '/' + twitter_api_endpoint
        self._twitter_api_resource = twitter_api_endpoint.split('/')[0]

        if logger is None:
            self._logger = get_console_info_logger()
        else:
            self._logger = logger

        self._update_rate_limit_status()

    def update_rate_limit_status(self):
        return self._update_rate_limit_status()

    def get_data(self, **twitter_api_parameters):
        """
        Retrieve data from the Twitter API endpoint associated with
        this class instance.

        This function can block for up to 15 minutes if the rate limit
        for this endpoint's window has already been reached.
        """
        return self._get_data_with_backoff(60, **twitter_api_parameters)


    def _get_data_with_backoff(self, backoff, **twitter_api_parameters):
        self._sleep_if_rate_limit_reached()
        self.api_calls_remaining_for_current_window -= 1
        try:
            return self._twython.get(self._twitter_api_endpoint, params=twitter_api_parameters)
        except TwythonError as e:
            self._logger.error("TwythonError: %s" % e)

            # Twitter error codes:
            #    https://dev.twitter.com/docs/error-codes-responses

            # Update rate limit status if exception is 'Too Many Requests'
            if e.error_code == 429:
                self._logger.error("Rate limit exceeded for '%s'. Number of expected remaining API calls for current window: %d" %
                                  (self._twitter_api_endpoint, self.api_calls_remaining_for_current_window + 1))
                time.sleep(backoff)
                self._update_rate_limit_status()
                return self._get_data_with_backoff(backoff*2, **twitter_api_parameters)
            # Sleep if Twitter servers are misbehaving
            elif e.error_code in [502, 503, 504]:
                self._logger.error("Twitter servers are misbehaving - sleeping for %d seconds" % backoff)
                time.sleep(backoff)
                return self._get_data_with_backoff(backoff*2, **twitter_api_parameters)
            # Sleep if Twitter servers returned an empty HTTPS response
            elif "Caused by <class 'httplib.BadStatusLine'>: ''" in str(e):
                # Twitter servers can sometimes return an empty HTTP response, e.g.:
                #   https://dev.twitter.com/discussions/20832
                #
                # The code currently detects empty HTTPS responses by checking for a particular
                # string:
                #   Caused by <class 'httplib.BadStatusLine'>: ''"
                # in the exception message text, which is fragile and definitely not ideal.  Twython
                # uses the Requests library, and the "Caused by %s: %s" string comes from the
                # version of urllib3 that is bundled with the Requests library.  Upgrading to a
                # newer version of the Requests library (this code tested with requests 2.0.0) may
                # break the detection of empty HTTPS responses.
                #
                # The httplib library (which is part of the Python Standard Library) throws the
                # httplib.BadStatusLine exception, which is caught by urllib3, and then re-thrown
                # (with the "Caused by" text) as a urllib3.MaxRetryError.  The Requests library
                # catches the urllib3.MaxRetryError and throws a requests.ConnectionError, and
                # Twython catches the requests.ConnectionError and throws a TwythonError exception -
                # which we catch in this function.
                self._logger.error("Received an empty HTTPS response from Twitter servers - sleeping for %d seconds" % backoff)
                time.sleep(backoff)
                return self._get_data_with_backoff(backoff*2, **twitter_api_parameters)
            # For all other TwythonErrors, reraise the exception
            else:
                raise e


    def _sleep_if_rate_limit_reached(self):
        if self.api_calls_remaining_for_current_window < 1:
            current_time = time.time()
            seconds_to_sleep = self._current_rate_limit_window_ends - current_time

            # Pad the sleep time by 15 seconds to compensate for possible clock skew
            seconds_to_sleep += 15

            # If the number of calls available is 0 and the rate limit window has already
            # expired, we sleep for 60 seconds before calling self._update_rate_limit_status()
            # again.
            #
            # In testing on 2013-11-06, the rate limit window could be expired for over a
            # minute before calls to the Twitter rate_limit_status API would return with
            # an updated window expiration timestamp and an updated (non-zero) count for
            # the number of API calls available.
            if seconds_to_sleep < 0:
                seconds_to_sleep = 60

            sleep_until = datetime.datetime.fromtimestamp(current_time + seconds_to_sleep).strftime("%Y-%m-%d %H:%M:%S")
            self._logger.info("Rate limit reached for '%s', sleeping for %.2f seconds (until %s)" % \
                                 (self._twitter_api_endpoint, seconds_to_sleep, sleep_until))
            time.sleep(seconds_to_sleep)

            self._update_rate_limit_status()

            # Recursion! Sleep some more if necessary after updating rate limit status
            self._sleep_if_rate_limit_reached()


    def _update_rate_limit_status(self):
        #  https://dev.twitter.com/docs/api/1.1/get/application/rate_limit_status
        rate_limit_status = self._twython.get_application_rate_limit_status(resources=self._twitter_api_resource)

        self._current_rate_limit_window_ends = rate_limit_status['resources'][self._twitter_api_resource][self._twitter_api_endpoint_with_prefix]['reset']

        self.api_calls_remaining_for_current_window = rate_limit_status['resources'][self._twitter_api_resource][self._twitter_api_endpoint_with_prefix]['remaining']

        dt = int(self._current_rate_limit_window_ends - time.time())
        rate_limit_ends = datetime.datetime.fromtimestamp(self._current_rate_limit_window_ends).strftime("%Y-%m-%d %H:%M:%S")
        self._logger.info("Rate limit status for '%s': %d calls remaining until %s (for next %d seconds)" % \
                             (self._twitter_api_endpoint, self.api_calls_remaining_for_current_window, rate_limit_ends, dt))


def get_connection( consumer_key, consumer_secret):
    ACCESS_TOKEN = Twython(consumer_key, consumer_secret, oauth_version=2).obtain_access_token()
    twython = Twython(consumer_key, access_token=ACCESS_TOKEN)
    return twython

def get_timeline_crawler( twython, logger=None):
    """Requires a Twython instance passed to it, obtain such
    from `get_connection`"""
    timeline_crawler = CrawlTwitterTimelines(twython, logger)
    return timeline_crawler

def get_friend_follower_crawler( twython, logger=None):
    """Requires a Twython instance passed to it, obtain such
    from `get_connection`"""
    ff_finder = FindFriendFollowers(twython, logger)
    return ff_finder

def get_follower_crawler( twython, logger=None):
    """Requires a Twython instance passed to it, obtain such
    from `get_connection`"""
    follower_finder = FindFollowers(twython, logger)
    return follower_finder

def get_followee_crawler( twython, logger=None):
    """Requires a Twython instance passed to it, obtain such
    from `get_connection`"""
    followee_finder = FindFollowees(twython, logger)
    return followee_finder

def get_list_membership_crawler(twython, logger=None):
    """Requires a Twython instance passed to it, obtain such
    from `get_connection`"""
    membership_finder = ListMembership(twython, logger)
    return membership_finder
