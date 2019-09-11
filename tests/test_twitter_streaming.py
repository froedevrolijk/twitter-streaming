import pytest
import csv
from src.twitterdata.twitter_streaming import convert_to_epoch, filter_tweets, extract_values
from tests.fixture import three_tweets


@pytest.mark.parametrize("test_input, expected_output", [
    (three_tweets[0]['created_at'], 1551260241),
    (three_tweets[1]['created_at'], 1551260254),
    (three_tweets[2]['created_at'], 1551260254),
    (three_tweets[0]['user']['created_at'], 1416241160),
    (three_tweets[1]['user']['created_at'], 1542019318),
    (three_tweets[2]['user']['created_at'], 1249050062),
])
def test_convert_to_epoch(test_input, expected_output):
    assert convert_to_epoch(test_input) == expected_output
    assert type(convert_to_epoch(test_input)) == int


def test_filter_tweets():
    expected = dict({'id': 'test', 'creation_date_msg': 'test', 'text': 'test', 'author': 'test',
                          'user_id': 'test', 'creation_date_user': 'test', 'user_name': 'test', 'screen_name': 'test'}).keys()
    output = filter_tweets(three_tweets[0])
    assert output.keys() == expected
    assert len(output.keys()) == 8
    assert type(output) == dict


def test_extract_values():
    expected = dict({'id': 'test', 'creation_date_msg': 'test', 'text': 'test', 'author': 'test',
                          'user_id': 'test', 'creation_date_user': 'test', 'user_name': 'test', 'screen_name': 'test'})
    single_dict_output = filter_tweets(three_tweets[0])
    list_of_dicts_output = extract_values(three_tweets)
    assert single_dict_output.keys() == expected.keys()
    assert len(single_dict_output) == 8
    assert type(list_of_dicts_output) == list


def test_write_to_file():
    expected_header = ['id', 'creation_date_msg', 'text', 'author', 'user_id', 'creation_date_user', 'user_name', 'screen_name']
    with open('src/output/tweets.txt') as tsvfile:
        reader = csv.DictReader(tsvfile, delimiter='\t')
        hdrs = reader.fieldnames
        line_nmb = reader.line_num
    assert hdrs == expected_header
    assert len(hdrs) == 8
    assert line_nmb <= 100
