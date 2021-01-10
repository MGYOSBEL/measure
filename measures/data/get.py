import json
import requests


class GetData(): 

    def __init__(self, api_url):
        response = requests.get(api_url)

        if response.status_code == 200:
            self.data = json.loads(response.content.decode('utf-8'))
        else:
            self.data = []
    

