import requests, json

response = requests.get('https://hacker-news.firebaseio.com/v0/newstories.json?print=pretty')
ids = json.loads(response.text)
for id in ids:
  print(id)