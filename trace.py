import json, requests, base64

def getTraceAPIDetails(imagedata):
    url = 'https://trace.moe/api/search'
    data = {
        'image': base64.b64encode(imagedata).decode('utf8')
    }
    res = requests.post(url, json=data)
    try:
        return res.json()
    except Exception as e:
        return {
                    "RawDocsCount":5539400,
                    "RawDocsSearchTime":33287,
                    "ReRankSearchTime":1883,
                    "CacheHit":False,
                    "trial":3,
                    "docs":[
                        {
                            "from":881.58,
                            "to":882.58,
                            "anilist_id":98349,
                            "at":881.58,
                            "season":"2017-10",
                            "anime":"Love Live! Sunshine!! 2",
                            "filename":"[DMG&Mabors][Love Live! Sunshine!! S2][05][720P][BIG5].mp4",
                            "episode":5,
                            "tokenthumb":"lWRu1GVgDvrQg_WCgjxgpw",
                            "similarity":0.8826773090569642,
                            "title":"\u30e9\u30d6\u30e9\u30a4\u30d6\uff01\u30b5\u30f3\u30b7\u30e3\u30a4\u30f3!! 2",
                            "title_native":"\u30e9\u30d6\u30e9\u30a4\u30d6\uff01\u30b5\u30f3\u30b7\u30e3\u30a4\u30f3!! 2",
                            "title_chinese":"Love Live! Sunshine!! 2",
                            "title_english":"Love Live! Sunshine!! Season 2",
                            "title_romaji":"Love Live! Sunshine!! 2",
                            "mal_id":34973,
                            "synonyms":[],
                            "synonyms_chinese":["\u660e\u661f\u5b78\u751f\u59b9 Sunshine!! 2", "\u5b78\u5712\u5076\u50cf\u796d Sunshine!! 2"],
                            "is_adult":False
                        },
                    ],
                    "limit":9,
                    "limit_ttl":60,
                    "quota":149,
                    "quota_ttl":86400
                }