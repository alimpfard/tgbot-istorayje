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
        return None 