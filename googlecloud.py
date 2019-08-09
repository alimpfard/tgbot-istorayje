import json, base64, requests

def getCloudAPIDetails(image_data):
    """
    Uses google's cloud api to get json data (unofficial)
    Capability:
        VERY LIMITED
    """

    url = "https://cxl-services.appspot.com/proxy?url=https%3A%2F%2Fvision.googleapis.com%2Fv1%2Fimages%3Aannotate"

    data = {
        "requests":[
            {
                "image":{
                    "content": base64.b64encode(image_data).decode('utf8')
                },
                "features":[
                    {
                    "type":"TYPE_UNSPECIFIED","maxResults":50
                    },
                    {
                    "type":"LANDMARK_DETECTION","maxResults":50
                    },
                    {
                    "type":"FACE_DETECTION","maxResults":50
                    },
                    {
                    "type":"LOGO_DETECTION","maxResults":50
                    },
                    {
                    "type":"LABEL_DETECTION","maxResults":50
                    },
                    {
                    "type":"DOCUMENT_TEXT_DETECTION","maxResults":50
                    },
                    {
                    "type":"SAFE_SEARCH_DETECTION","maxResults":50
                    },
                    {
                    "type":"IMAGE_PROPERTIES","maxResults":50
                    },
                    {
                    "type":"CROP_HINTS","maxResults":50
                    },
                    {
                    "type":"WEB_DETECTION","maxResults":50
                    }
                ],
                "imageContext":{
                    "cropHintsParams":{
                        "aspectRatios":[0.8,1,1.2]
                    }
                }
            }
        ]
    }

    r = requests.post(url,json=data)
    print(r)
    return r.json()

