#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
import requests
import time
import datetime
import pymongo
import logging
import logging.config
from bs4 import BeautifulSoup
import random

LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s'
        },
        'simple': {
            'format': '%(asctime)s -- %(name)s !!!%(levelname)s!!!: %(message)s'
        },
    },

    'handlers': {
        'file': {
            'level': 'DEBUG',
            'formatter': 'simple',
            'class': 'logging.FileHandler',
            'filename': 'run.log',
            'mode': 'a',
        },
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'simple'
        },
        'database': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'formatter': 'simple',
            'filename': 'run.log',
            'mode': 'a',
        }
    },

    'loggers': {
        'mongo': {
            'handlers':['file'],
            'propagate': True,
            'level':'DEBUG',
        },
    },

    'root': {
        'level': 'DEBUG',
        'handlers': ['console']
    }
}

#mongo_client = pymongo.MongoClient(host='10.116.116.51', port=27018, replicaset='rs0')
mongo_client = pymongo.MongoClient('18.8.8.209')
cy = mongo_client.cy

cy_category = pd.DataFrame(list(cy.cy_cato.find(projection={'_id': False})))


google_play_url = "https://play.google.com"
base_url = 'https://play.google.com//store/apps/details?id='

user_agent = ["Mozilla/5.0(Macintosh;U;IntelMacOSX10_6_8;en-us)AppleWebKit/534.50(KHTML,likeGecko)Version/5.1Safari/534.50",
                "Mozilla/5.0(Windows;U;WindowsNT6.1;en-us)AppleWebKit/534.50(KHTML,likeGecko)Version/5.1Safari/534.50",
                "Mozilla/5.0(compatible;MSIE9.0;WindowsNT6.1;Trident/5.0",
                "Mozilla/4.0(compatible;MSIE8.0;WindowsNT6.0;Trident/4.0)",
                "Mozilla/4.0(compatible;MSIE7.0;WindowsNT6.0)",
                "Mozilla/4.0(compatible;MSIE6.0;WindowsNT5.1)",
                "Mozilla/5.0(Macintosh;IntelMacOSX10.6;rv:2.0.1)Gecko/20100101Firefox/4.0.1",
                "Mozilla/5.0(WindowsNT6.1;rv:2.0.1)Gecko/20100101Firefox/4.0.1",
                "Opera/9.80(Macintosh;IntelMacOSX10.6.8;U;en)Presto/2.8.131Version/11.11",
                "Opera/9.80(WindowsNT6.1;U;en)Presto/2.8.131Version/11.11",
                "Mozilla/5.0(Macintosh;IntelMacOSX10_7_0)AppleWebKit/535.11(KHTML,likeGecko)Chrome/17.0.963.56Safari/535.11",
                "Mozilla/4.0(compatible;MSIE7.0;WindowsNT5.1;Maxthon2.0)",
                "Mozilla/4.0(compatible;MSIE7.0;WindowsNT5.1;TencentTraveler4.0)"]
headers = {
    'accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    'accept-encoding': "gzip, deflate, br",
    'accept-language': "en;",
    'cache-control': "no-cache",
    'cookie': "HSID=AG82OPMBeuNmdVcpg; SSID=AlqY-W6cRxtxlGig5; APISID=2Cll8EW--jaFAmUu/AsKJC_t9XbEQRfBvd; SAPISID=4eJc6hPRSxRrqBr6/AfeoBHGWoMOZZs2tE; SID=3AWVKgajPAb3-nflI6GmAzYykB7aAP2tsImb-hUfnWNjwbQ6_Sxz5ERlV76Kp1oYdbsFnw.; PLAY_ACTIVE_ACCOUNT=ICrt_XL61NBE_S0rhk8RpG0k65e0XwQVdDlvB6kxiQ8=amit.luu@gmail.com; _ga=GA1.3.1314769735.1523435478; OTZ=4352192_24_24__24_; SIDCC=AEfoLebf52D4nndXoovcdyTN-SfxtIhdxEsC1Mb6oCkFDvAP_AzD_G1w1TFctH0oZ89FeUxHyg; NID=128=fOd4E_zJKKPMDx2Y9K5l64SkyeuxtEAdFoSXI9KtxJKYVGykt5xW4xxyujI6A5Nh3DG0dKGTt921GT95RLZY936K_spX5wZD4wYWQl37qkK-lwDnS1FgO4vNgL43a1AUGCsHlrJXFdF6V8s2HFOeq6l3MIepKKdYAcspBlYUGAVqjZTOX8grJLYc7htJBcV3Peu7lPwmXy28KYdCa3yp5jXZ0OEcd36dFJmOBKc0MORtjbkcPeNueEGfc23_REl659BiZ7Rg1ZRSNQ7rz6ov3UpbOSX7; 1P_JAR=2018-4-24-1; _gid=GA1.3.138382773.1524533993; _gat_UA199959031=1; PLAY_PREFS=CqIMCJXVxOfqGBKYDAoCVFcQnMKwkq0sGtkLEhMUFRjxAasC4QPCBOMF5QXoBdcGkJWBBpGVgQaSlYEGlZWBBpeVgQaklYEGuJWBBsCVgQbBlYEGxJWBBsWVgQbIlYEGyZWBBs6VgQbPlYEG0JWBBtSVgQbYlYEG3pWBBvGVgQb4lYEG-pWBBoaWgQaIloEGjJaBBo-WgQaSloEGnpaBBp-WgQahloEGpJaBBqaWgQanloEG7peBBu-XgQaBmIEGhZiBBomYgQa-mIEGq5uBBq2bgQbIm4EGypuBBsubgQbVm4EGvJ2BBt2dgQbnnYEGkJ6BBuKigQbzooEGi6OBBpqkgQaypIEG6qWBBsamgQbPp4EGvKyBBtavgQaHsoEGibKBBtaygQbJs4EGsbSBBta5gQaOwIEGosCBBsDAgQbBwIEG8sCBBtbCgQaMxYEG-MeBBqrKgQbYzIEG3MyBBt3NgQaGzoEGoc-BBsTSgQaV1YEG2tiBBuLYgQaT2YEG8tuBBtjkgQbc5IEGl-WBBrjogQbP64EGsOyBBtf1gQa6-4EGv_-BBsX_gQbH_4EGyP-BBtWDggbIhIIGuYaCBqaHgganh4IG7IeCBu2Hggb7jYIGiY6CBsyRggaVmIIGj5qCBpmaggbBmoIG95qCBp2eggbVnoIGuqCCBrugggb2ooIG4qSCBpKlggarpYIGzaWCBvKnggaeqIIGtKiCBq22ggb8uYIG_rmCBv-5ggbCu4IGj7-CBurAgga8wYIGkMuCBpHLggbNy4IG0cuCBtzMggbY0IIG89GCBovSggbb04IGgdiCBoXaggaP2oIGmtqCBqPaggau24IGvtuCBsXbggaW3IIGsdyCBurdggb43YIG79-CBqbhggbQ4YIG5OGCBuXhggaW6YIGo-2CBoXuggaa7oIGs-6CBozwggax8IIGlvGCBr7xggbr9oIGrfiCBrP4ggb2-oIG3vuCBt_7ggbj-4IGg_yCBoX8ggbb_IIG3PyCBv_8ggaB_YIGgv-CBoCAgwaJgYMG8oGDBoGCgwbphIMGkIWDBrqFgwabiIMG0IiDBvCIgwbzjYMGhY-DBpCPgwbZkYMG_JGDBvySgwaslYMGuJWDBsCWgwbkloMG3JeDBvGZgwb0mYMG25qDBpmbgwbtm4MG7puDBtCcgwb0noMGlZ-DBsafgwbTn4MGmKCDBpuggwb9oIMGuaODBuCugwbsr4MGiLCDBpawgwaYtIMGuLaDBqS3gwaAuIMGr7iDBrm4gwbDuIMG4LyDBvS8gwb2vIMGrL6DBs2-gwa-wIMGsMGDBvLBgwbGwoMGycKDBuTGgwatyIMGnsmDBpvKgwaHzYMGys2DBv7Ngwai0IMG6NCDBsDTgwb504MG69SDBofVgwbP1oMG69aDBo3Xgwbj14MG6deDBtHZgwas3IMGgt-DBpTfgwbh34MGhuCDBojggwaa4YMG7-ODBv_lgwaR5oMG5-iDBsHpgwbJ7IMG6uyDBo7tgwaV7oMG4--DBpzygwb79IMGrfeDBq73gwav94MG8PeDBvn3gwaM-IMGjfiDBpf4gwaY-IMG1vmDBt75gwac-oMGsfqDBsL6gwaH_4MGkYCEBpKAhAajgIQG1YGEBt2BhAbfgYQG-oOEBsOEhAbXhIQGloeEBuKHhAbjh4QGyYiEBqSJhAa-iYQG-4qEBqeLhAbCi4QGmoyEBp2MhAbzjIQGgI6EBoeOhAaTjoQGm4-EBu6RhAb8kYQG3JKEBu-ThAafl4QGyJeEBs6XhAaJmIQG85iEBoaZhAabmYQGopmEBruZhAbOm4QG-JuEBoechAacnIQG35yEBuCchAaBnYQGlZ2EBtudhAacnoQGs56EBvOehAbgn4QGuqCEBtehhAbwo4QGi6SEBrGkhAatpYQG_6eEBqmohAbZqIQG9KiEBqCphAamqYQGqKmEBqqphAasqYQGramEBq-phAaxqYQGna-EBqevhAapsIQGt7GEBrixhAbpsYQGtbOEBre0hAbJtIQGKLLCsJKtLDokMzc1MDg1ZGItYzQ3MS00Y2JjLTk0OTEtOWJkYjAzNzg4ODg3QAFIAArDCQgAEr4JCgJVUxCMj7uqrywa_wgREhMUFdQB1QGnAsQE4wXlBegF1wbYBt4G3waQlYEGkZWBBpKVgQaXlYEGt5WBBriVgQbAlYEGwZWBBsSVgQbHlYEG1JWBBtmVgQbylYEG-JWBBpuWgQadloEGnpaBBp-WgQagloEG7peBBoWYgQaJmIEGipiBBouYgQa-mIEGq5uBBq2bgQbJm4EGypuBBsubgQbVm4EGvJ2BBt2dgQbnnYEGkJ6BBuKigQbzooEG_KKBBoujgQaapIEG6qWBBsamgQbUpoEG1aaBBtamgQb-poEGgKeBBoKngQaEp4EGhqeBBoingQaKp4EGzqiBBvKogQb0qIEGvKyBBtavgQbBsIEGpLGBBqWxgQaHsoEGibKBBtaygQaxtIEGv7mBBta5gQaiwIEGwMCBBsHAgQbywIEGwcGBBtbCgQaMxYEGj8WBBsrGgQbLxoEG-MeBBqrKgQbYzIEG3MyBBt3NgQaGzoEGoc-BBsTSgQaV1YEG2tiBBuLYgQbL2YEG8tuBBtjkgQaX5YEGuOiBBs_rgQaw7IEG1_WBBrr7gQa7_4EGyf-BBtWDggbIhIIGuYaCBqaHgganh4IGs4eCBuyHggbth4IG642CBvuNggaJjoIGj5GCBsuRggaVmIIGj5qCBpmaggbBmoIG95qCBp2eggbVnoIGu6CCBvaiggbipIIGkqWCBvKnggaeqIIGtKiCBtG1ggattoIG_LmCBv65ggb_uYIGwruCBo-_ggbqwIIGvMGCBpDLggaRy4IGzcuCBtHLggbczIIG2NCCBvPRggaL0oIGgdiCBqPagga-24IGxduCBrHcggbq3YIG-N2CBu_fggbQ4YIG0eGCBuXhggaW6YIGo-2CBoXuggaz7oIGjPCCBrHwgga-8YIG6_aCBq34ggaz-IIG9vqCBt_7ggbj-4IGg_yCBoX8ggbb_IIG3PyCBv_8ggaB_YIGgv-CBoCAgwaJgYMG8oGDBoGCgwaQhYMGuoWDBteHgwabiIMG0IiDBvCIgwaFj4MGkI-DBtmRgwb8koMGrJWDBriVgwbAloMG3JeDBtuagwbtm4MG7puDBtCcgwb0noMGlZ-DBsafgwaboIMG_aCDBrmjgwbgroMG7K-DBpW0gwaYtIMGrbSDBri2gwbgvIMG9LyDBva8gwbNvoMGsMGDBsnCgwa4xoMG5MaDBq3IgwaeyYMGm8qDBvXLgwaHzYMGys2DBv7Ngwai0IMGutCDBujQgwb504MG69SDBofVgwbP1oMG69aDBuPXgwbR2YMGrNyDBoLfgwbh34MGhuCDBojggwbv44MG_-WDBsnsgwaO7YMGle6DBuPvgwad9IMG1fSDBvv0gwat94MGr_eDBo34gwaY-IMG1vmDBt75gwaM_oMGh_-DBpGAhAaSgIQGp4GEBt2BhAbfgYQG-oOEBteEhAbih4QG44eEBqSJhAa-iYQG-4qEBoCOhAabj4QG7pGEBvyRhAbOl4QGiZiEBoGdhAaKnYQGraWEBv-nhAanr4QGwa-EBvevhAb4r4QGyLOEBiiVj7uqryw6JGUzYTA0MDU4LTFhNzctNDJkOC1iZGE5LTViMjAyNTUyNDkxN0ABSAA:S:ANO1ljJJHodwfTYeaQ; _gat=1; S=billing-ui-v3=HRQ8KRXii0u-sR1m9xu_yBlmQf8khkXJ:billing-ui-v3-efe=HRQ8KRXii0u-sR1m9xu_yBlmQf8khkXJ",
    'upgrade-insecure-requests': "1",
    'user-agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36",
    'x-client-data': "CKe1yQEIibbJAQiltskBCMG2yQEIq5jKAQipncoBCN6eygEIqKPKARiSo8oB",
}


def get_category(app):
    my_logger = logging.getLogger('mongo')
    # check database first:
    category = cy.topappies.find_one({'package_name': app['package_name']})

    if category is not None:
        app['category'] = category['category']
        app['cy_category'] = category['cy_category']
        app['category_id'] = category['category_id']
        my_logger.debug('{}:got it from database'.format(app['package_name']))
        return True
    else:
        detail_url = base_url + app['package_name']
        my_logger.debug(detail_url)

        try:
            response = requests.request("GET", detail_url, headers=headers)
        except Exception as err:
            my_logger.error('{}:request exception:'.format(app['package_name']))
            my_logger.error(err)
            return False

        if response.status_code != 200:
            my_logger.debug('{}:got failed with status code:{}'.format(app['package_name'], response.status_code))
            if response.status_code == 404:
                #set default value:
                app['category'] = 'TOOLS'
                app['cy_category'] = 'Tools'
                app['category_id'] = 1
        else:
            soup = BeautifulSoup(response.text, "lxml")
            hrefs = soup.find_all('a')
            for href in hrefs:
                try:
                    if href.get('itemprop')  == 'genre':
                        app['category'] = href['href'].split('/')[-1]
                        try:
                            app['cy_category'] = cy_category[cy_category['category'] == app['category']]['title'].values[0]
                            app['category_id'] = int(cy_category[cy_category['category'] == app['category']]['id'].values[0])
                        except IndexError:
                            my_logger.error('{}:category {} not found in cy category'.format(
                                app['package_name'], app['category']))
                            app['cy_category'] = 'Tools'
                            app['category_id'] = 1
                        my_logger.debug('{}: get category from google play succuessfully'.format(app['package_name']))
                        return True
                except:
                    my_logger.debug('{}:parse failed'.format(app['package_name']))
                    pass

    return False

def get_topapps(country):
    url = "https://www.appannie.com/ajax/top-chart/table/"
    now = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    my_logger = logging.getLogger('mongo')
    my_logger.debug("starting get topapps for country:[{}] at {}".format(country,now))
    
    querystring = {"market": "google-play", "country_code": country, "category": "1",
                   "date": now, "rank_sorting_type": "rank", "page_size": "500", "order_type": "desc"}
    rank_country = country.lower()
    headers_appannie = {
                'accept': "application/json, text/plain, */*",
                'accept-encoding': "gzip, deflate, br",
                'accept-language': "zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7",
                'cookie': "optimizelyEndUserId=oeu1528194190178r0.8089536040737322; _ga=GA1.2.778478037.1528194191; _gid=GA1.2.226148796.1528194191; _mkto_trk=id:071-QED-284&token:_mch-appannie.com-1528194190588-56981; _tdim=a9e43662-9751-4b9b-f32e-28f40fd414f6; csrftoken=RQ3Yxttqwsehh3x7i17NDvR3MTgc2d9S; _hp2_ses_props.3646280627=%7B%22r%22%3A%22https%3A%2F%2Fwww.appannie.com%2Faccount%2Fregister%2F%3F_ref%3Dheader%22%2C%22ts%22%3A1528201539717%2C%22d%22%3A%22www.appannie.com%22%2C%22h%22%3A%22%2Faccount%2Fverify-email%2F%22%7D; welcome_carousel=1; rid=9028d36de21a4733ab7ebc895e42ec4b; _hp2_props.3646280627=%7B%22Logged%20In%22%3A%22true%22%2C%22aa_user_id%22%3A1400839%2C%22Email%22%3A%22luusuu%40126.com%22%2C%22AA%20User%20Type%20Long%22%3A%22%22%2C%22AA%20User%20is%20Verified%22%3A%22true%22%2C%22AA%20User%20Preferred%20Language%22%3A%22zh-cn%22%2C%22AA%20User%20is%20Intelligence%22%3A%22false%22%2C%22AA%20User%20is%20Store%20Intelligence%22%3A%22false%22%2C%22AA%20User%20is%20Usage%20Intelligence%22%3A%22false%22%2C%22AA%20User%20is%20Audience%20Intelligence%22%3A%22false%22%2C%22AA%20User%20is%20Marketing%20Intelligence%22%3A%22false%22%2C%22AA%20User%20is%20Intelligence%20API%22%3A%22false%22%2C%22AA%20User%20is%20Newsletter%20Subscriber%22%3A%22true%22%2C%22AA%20User%20is%20Blog%20Subscriber%22%3A%22false%22%7D; aa_user_token=\".eJxNzD0LAiEYAOC6imhoCdobmw7PNHKtqaDR2d57fUHp8DC1jyHop7c09AOe51O943CtZwXz0zhILlZ6pQC2DeMgJAMSILmSjKOUgjPLd2qjFwZKdqYkupkW8ErBxpFmD2ohQPfKHlMNiH0JuT5AomNIFJLP_k7n3lK3_5mxnv9N3sbJaXpZDkr9BWLCMlA:1fQB3l:KhNcfjcHqC3MdSeiXlBh0-dF9ow\"; aa_language=en; sessionId=\".eJxNzLtqAkEUgOF14yWs8ULAwt5Cm8Vn0CqRNJIBq0zOzhx0cDzreM4oCoKVkNf0SYIQJN3X_P8lPYfKULU0RFnpyLjTzr43vnuJevZAywhLXKRJkiCFVHUYmV1JGgkKj3aWqqaUrOPWgqD9Ua__PgWYNZJV4wMWQOCP4gznYEwZSfIpML4RI7ETt8eP0qKf_BVd8LgTbVZo1lrcBs19f0f2QHhSWeNWr730B-3Bl9ke5ZRp9TnNQnU0D7XrPNRj_guhU0yD:1fQB48:nUGNmXTPUo-cc6RBqTZinOtBXek\"; django_language=en; aa-cookie-done=1; wcs_bt=s_14249a956b4d:1528201845; _hp2_id.3646280627=%7B%22userId%22%3A%228111173746324231%22%2C%22pageviewId%22%3A%222061653134642730%22%2C%22sessionId%22%3A%225275403080355129%22%2C%22identity%22%3A%221400839%22%2C%22trackerVersion%22%3A%224.0%22%2C%22identityField%22%3Anull%2C%22isIdentified%22%3A1%7D",
                'referer': "https://www.appannie.com/apps/google-play/top-chart/?country=AR&category=1&device=&date=2018-06-05&feed=Free&rank_sorting_type=rank&page_number=0&page_size=300&table_selections=&order_type=desc&order_by=free_rank",
                'user-agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.62 Safari/537.36",
                'x-csrftoken': "RQ3Yxttqwsehh3x7i17NDvR3MTgc2d9S",
                'x-newrelic-id': "VwcPUFJXGwEBUlJSDgc=",
                'x-requested-with': "XMLHttpRequest",
                'cache-control': "no-cache",
    }

    headers_appannie['user-agent'] = user_agent[random.randrange(0, len(user_agent) - 1)]
    response = requests.request(
        "GET", url, headers=headers_appannie, params=querystring)
    my_logger.debug("Get {} return with status code:{}".format(country, response.status_code))

    if response.status_code == 200:
        all_rows = response.json()['table']['rows']
        rank = 1
        for row in all_rows:
            cells = row[1:3]
            for cell in cells:
                app = cell[0]
                try:
                    app['package_name'] = app['url'].split('/')[-3]
                except IndexError:
                    my_logger.debug("{}: get package name at rank {} error for country:{}, skippp it".format(app, rank, rank_country))
                    rank = rank + 1
                    continue
                app['rank_country'] = rank_country
                app['rank'] = rank
                rank = rank + 1
                my_logger.debug('{}: starting get category from google play'.format(app['package_name']))
                if get_category(app):
                    cy.topappies.update_one({'package_name': app['package_name'], 'rank_country': rank_country}, {'$set': app}, upsert=True)
                else:
                    #try one more time:
                    my_logger.error('{}: get category from google play failed, try again'.format(app['package_name']))
                    if get_category(app):
                        cy.topappies.update_one({'package_name': app['package_name'], 'rank_country': rank_country}, {'$set': app}, upsert=True)
                    else:
                        my_logger.error('{}: get category from google play failed again'.format(app['package_name']))


def rank_category(apps):
    my_logger = logging.getLogger('mongo')
    for app in apps:
        try:
            app['cy_category'] = cy_category[cy_category['category']
                                             == app['category']]['title'].values[0]
        except IndexError:
            my_logger.error('{}:category {} not found in cy category'.format(
                app['package_name'], app['category']))
            app['cy_category'] = 'Tools'
    df = pd.DataFrame(apps)
    cy_category_sort = df.groupby('cy_category').count()[
        'package_name'].sort_values(ascending=False)
    category_rank = dict(
        zip(cy_category_sort.index, range(1, len(cy_category_sort.index)+1)))

    for app in apps:
        app['category_id'] = category_rank[app['cy_category']]
        cy.topappies.update_one({'package_name': app['package_name'], 'rank_country': app['rank_country']}, 
                                {"$set": {"category_id": app['category_id'], 'cy_category':app['cy_category']}})

if __name__ == '__main__':
    logging.config.dictConfig(LOGGING)
    my_logger = logging.getLogger('mongo')
    
    df = pd.DataFrame(list(cy.googleplay_country.find(projection={'_id': False, 'country_name': False})))
    countries = list(df['country_code'])

    
    for country in countries:
        get_topapps(country)
        my_logger.debug("sleep finish getting topapp for " + country)
        time.sleep(random.randrange(120, 240))
        my_logger.debug("finish getting topapp for " + country)
    

    '''
    for country in countries:
        my_logger.debug("starting rank category for " + country.lower())
        apps = list(cy.topappies.find({'rank_country':country.lower()}))
        rank_category(apps)
    '''
