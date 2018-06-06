#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
import requests
import time
import datetime
from pymongo import *
import logging
import logging.config
from bs4 import BeautifulSoup

def get_google_play_countries():
    headers = {'Authorization': 'Bearer d05683c381a2db185f7340022d06efbd1f9911a6'}
    url = 'https://api.appannie.com/v1.2/meta/countries'
    response = requests.request("GET", url, headers=headers)

    if response.status_code != 200:
        return None
        
    df = pd.DataFrame(response.json()['country_list'])
    soup = BeautifulSoup(open('country.html', encoding='utf-8'), "lxml")
    countries = soup.find_all('span', attrs={'once-text': 'c.label'})
    country_names = [x.string for x in countries]
    google_play_countries = df[df.country_name.isin(country_names)]

    return google_play_countries


if __name__ == '__main__':
    client = MongoClient(host='10.116.116.51', port=27018, replicaset='rs0')
    db = client.cy

    google_play_country = get_google_play_countries()

    if google_play_country is not None:
        db.googleplay_country.insert_many(google_play_country.to_dict(orient='record'))

