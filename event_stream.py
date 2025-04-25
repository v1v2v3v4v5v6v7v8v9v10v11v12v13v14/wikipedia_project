import json
import requests
params = {
    'q':             'mediatype:data AND identifier:enwiki-*',
    'rows':          0,
    'facet':         'true',
    'facet.field[]': 'year',      # note the []
    'facet.limit[]': -1,          # same idea
    'facet.sort[]':  'count',
    'output':        'json'
}
r = requests.get('https://archive.org/advancedsearch.php', params=params)
print(r)
print("URL:", r.url)
print("Keys:", r.json().keys())
# then as above, inspect r.json()['facet_counts']