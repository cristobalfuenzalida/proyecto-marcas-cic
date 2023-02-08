import requests
import json

AUTH = {
    'Authorization': 'Token 9695e4167ada53df67b5d9cb24d0c0e1cb6ff288'
}
response_api = requests.get(
    'https://talana.com/es/api/persona/?rut=8837478-9',
    headers=AUTH
)

rut_ejemplo = '8837478-9'
id_ejemplo = '1494137'

