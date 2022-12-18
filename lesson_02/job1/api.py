API_URL = 'https://fake-api-vycpfa6oca-uc.a.run.app/'

import requests
def get_sales(date: str, auth_token: str):
    """
    gets data from the API
    essentially ignores API errors and does not raise them, instead returns an empty result
    if f.e. an incorrect date has been provided API would return 404, but we would just return empty result
    date sanity checks should either be done in the higher layers, or we should rework into raising exceptions later
    """
    page = 0
    result = []
    while True:
        page+=1
        response = requests.get(API_URL, params={'date': date, 'page': page}, headers={'Authorization', auth_token})
        if response.status_code == 200:
            json = response.json()
            if isinstance(json, list) and len(json) > 0:
                result += json
            else:
                break
        else:
            break

    return result
