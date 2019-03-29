import os
import requests
import base64
import logging
import json
import time
import boto3
from datetime import datetime
from requests.auth import HTTPBasicAuth


def store_state(name, state):
    """Set the state of the lambda in SSM
    
    Args:
        name (str): the name of the state to grab
        state (dict): a dict of the state
    """
    ssm = boto3.client("ssm", "us-east-2")
    ssm.put_parameter(
        Name="get_tweet_replies_params",
        Description="state for get_tweet_replies",
        Value=json.dumps(state),
    )


def get_state(name):
    """Get the state back out of the lambda.  
    
    Args:
        name (str): The name of the state in AWS SSM
    
    Returns:
        dict
    """
    if name == 'test':
        return test_state
    ssm = boto3.client("ssm", "us-east-2")
    return json.loads(ssm.get_parameter(Name="get_tweet_replies_params"))


def get_token(key, secret):
    post_headers = {
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"
    }
    print(post_headers)
    res = requests.post(
        url="https://api.twitter.com/oauth2/token",
        data={"grant_type": "client_credentials"},
        headers=post_headers,
        auth=HTTPBasicAuth(key, secret)
    )
    print(res.json())
    return res.json()["access_token"]


def get_rate_limit_status(token):
    rate_limit = requests.get(
        "https://api.twitter.com/1.1/application/rate_limit_status.json?resources=search",
        headers={"Authorization": "Bearer {}".format(token)},
    )

    if rate_limit.status_code == 429:
        return {"remaining": 0, "reset": int(rate_limit.headers["x-rate-limit-reset"])}
    try:
        return rate_limit.json()["resources"]["search"]["/search/tweets"]
    except KeyError:
        print(rate_limit.json())
        raise


def get_available_token(creds):
    for c in creds:
        t = get_token(c["consumer_key"], c["consumer_secret"])
        rl = get_rate_limit_status(t)
        if rl["remaining"] > 100:
            return t


def make_request(token, query):
    url = "https://api.twitter.com/1.1/search/tweets.json"

    r = requests.get(
        url, headers={"Authorization": "Bearer {}".format(token)}, params=query
    )
    if not r.ok:
        logging.getLogger().info(url)
        logging.getLogger().info(r.text)
        logging.getLogger().info(r)
    r.raise_for_status()
    return r.json()


def get_last_status_id(token, state):
    query = {"q": state["q"], "count": state["count"], "result_type": "recent"}
    them = make_request(token, query)
    return max([x["id"] for x in them["statuses"]])

def write_out_statuses(statuses, state):
    st = json.dumps(statuses)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(state['bucket'])
    bucket.put_object(
        ACL='private',
        Body=st.encode('utf-8'),
        ContentType='application/json',
        Key='lambda-' + datetime.now().strftime('%Y%M%d')
    )

def get_tweet_replies(token, state, max_id):
    q = {
        "q": state["q"],
        "since_id": state["lastMaxId"],
        "max_id": max_id,
        "count": 100,
        "result_type": "mixed",
    }
    r = requests.get(
        "https://api.twitter.com/1.1/search/tweets.json",
        headers={"Authorization": "Bearer {}".format(token)},
        params=q,
    )
    statuses = r.json()["statuses"]
    while r.json()["search_metadata"].get("next_results"):
        r = requests.get(
            "https://api.twitter.com/1.1/search/tweets.json"
            + r.json()["search_metadata"].get("next_results"),
            headers={"Authorization": "Bearer {}".format(token)},
        )
        j = r.json()
        statuses += j['statuses']
        state['start_id'] = max([x['id'] for x in j['statuses']])
    write_out_statuses(statuses, state)
    return state


def get_tweets(event, context):
    ssm_name = event["ssm_name"]
    state = get_state(ssm_name)
    token = get_available_token(state["twitterCreds"])
    max_id = get_last_status_id(token, state)
    state = get_tweet_replies(token, state, max_id)
    store_state(ssm_name, state)


if __name__ == '__main__':
    test_state = {
        'q': 'to:realDonaldTrump',
        'count': 100,
        'lastMaxId': '1111640220603215872',
        'bucket': 'nowhere',
        'twitterCreds': [

            {
                "consumer_key": "KQ5KLpqEVVvbBgqZIWPwjgWsA",
                "consumer_secret": "ntOrdHTNH6UrCO6vvkcyqsENP4j57kCvJXlOtl4OPfgMjPjw9C",
                "access_token": "1000780949808930816-Ocr7H9fYicL71QWEzfDnWXVH55GLrS",
                "access_secret": "3fzZtsieOiNPlfbxtvc7cs8SUFfp40mGrmJPHnIEZoyrQ"
            },
                        {
                "consumer_key": "aUdBQs3tvGlcHaJyodyMFCC5D",
                "consumer_secret": "h6jJr7zXrBVdpf3ebzWNoKBq7Y6sbgM3S6kuVCixtHdnfkZHEA",
                "access_token": "000780949808930816-ynikc2Z5sA8fGveGLNXzDX3Mcn3eBK",
                "access_secret": "JdasD4N4d0ZOSRhkx2eQzuc9B6Z892KzHatQNwyu5SO2k"
            },
            {
                "consumer_key": "iphN9eet6Q85y1myvKDEvIU37",
                "consumer_secret": "TTMIVAyixLASrXEe52OVObYmhF7HvfnG5RrbAzowquyktgkfmW",
                "access_token": "1000780949808930816-735ipKf9XzhgLCnaueR7NgwABW8jwE",
                "access_secret": "xT2Ow1Qph8eMadIctw0RQdUbymr0jo6Nlu3v3tUGgZioi"
            }
        ]
    }
    get_tweets(event={"ssm_name": 'test'}, context=None)