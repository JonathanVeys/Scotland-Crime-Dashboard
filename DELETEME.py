import requests


crime_data = requests.get('http://127.0.0.1:8000/history/crime?ward_code=S13002517')

if crime_data.status_code == 200:
    print(crime_data.text)
else:
    print(crime_data.status_code)
