from requests import exceptions
from requests_oauthlib import OAuth1
from requests import Session
from requests.adapters import HTTPAdapter
from operator import itemgetter
import json
import urllib3
import time
import csv

from src.config.config import CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def twitter_request(url, auth, headers, data_parameters):
    """ Connect to the Twitter API and obtain streaming data """
    retries = urllib3.util.Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    with Session() as session:
        try:
            session.mount('https://', HTTPAdapter(max_retries=retries))
            response = session.request('POST', url, data=data_parameters,
                                       stream=True, headers=headers, auth=auth, verify=False)
            if response.status_code == 200:
                return response
            else:
                raise Exception('Got status code back other than 200')
        except exceptions.RequestException as e:
            print('Request error({0}): {1}'.format(e.errno, e.strerror))


def read_stream(response, max_tweets, read_time):
    """ Read the messages from the Twitter API """
    list_of_dicts = []
    timeout = time.time() + read_time
    print('Started reading from Twitter...')
    for tweet in response.iter_lines():
        try:
            if time.time() < timeout:
                tweet = json.loads(tweet)
                list_of_dicts.append(tweet)
                max_tweets -= 1

                if max_tweets == 0:
                    print('Done reading from Twitter.')
                    return list_of_dicts
            else:
                print('Done reading from Twitter.')
                return list_of_dicts
        except Exception as e:
            print('Could not read from Twitter.', e)


def convert_to_epoch(twitter_time):
    """ Convert the Twitter datetime pattern to an epoch value """
    twitter_pattern = '%a %b %d %H:%M:%S +0000 %Y'
    datetime_pattern = '%Y-%m-%d %H:%M:%S'
    try:
        date_time = time.strftime(datetime_pattern, time.strptime(twitter_time, twitter_pattern))
        return int(time.mktime(time.strptime(date_time, datetime_pattern)))
    except Exception as e:
        print('Not able to convert date value', e)


def filter_tweets(tweet):
    """ Filter the tweets to obtain only the desired values """
    try:
        return {
            'id': tweet['id'],
            'creation_date_msg': convert_to_epoch(tweet['created_at']),
            'text': tweet['text'],
            'author': tweet['user']['name'],

            'user_id': tweet['user']['id'],
            'creation_date_user': convert_to_epoch(tweet['user']['created_at']),
            'user_name': tweet['user']['name'],
            'screen_name': tweet['user']['screen_name']
    }
    except KeyError as e:
        print(e)


def extract_values(tweets):
    """ Loop through list if dictionaries and apply the filter_tweets function on it """
    output_list = []
    try:
        for tweet in tweets:
            output = filter_tweets(tweet)
            output_list.append(output.copy())
            output_list_sorted = sorted(output_list, key=itemgetter('author', 'text'))
        return output_list_sorted
    except Exception as e:
        print(e)


def write_to_file(dictionary, filename, mode, fieldnames, delimiter):
    """ Write the output of the dictionaries to a tab separated .txt file """
    try:
        with open(filename, mode) as output_file:
            writer = csv.DictWriter(output_file, fieldnames=fieldnames, delimiter=delimiter)
            writer.writeheader()
            writer.writerows(dictionary)
            output_file.close()
            print('Done writing to file. Please find the output in your project folder under: src/output/tweets.txt')
    except PermissionError as e:
        print(e)
    except IOError as e:
        print(e)
    finally:
        output_file.close()


if __name__ == '__main__':
    resp = twitter_request('https://stream.twitter.com/1.1/statuses/filter.json?',
                           OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET),
                           {"Accept": "application/json"},
                           {'Name': 'description', 'track': 'bieber'})

    unfiltered_tweets = read_stream(resp, max_tweets=100, read_time=30)

    filtered_tweets = extract_values(unfiltered_tweets)

    write_to_file(filtered_tweets, '../output/tweets.txt', 'w', ['id', 'creation_date_msg', 'text', 'author',
                  'user_id', 'creation_date_user', 'user_name', 'screen_name'], '\t')
