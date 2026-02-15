import os, json

with open('all_filtered_data.json') as f:
    data = json.load(f)

filtered = {}

for d in data:
    if len(d['name']) <= 5:
        continue
    if d['name'] in filtered:
        filtered[d['name']]['school'].append(d['school'])
        filtered[d['name']]['cnt'] += d['data']['frequency']
    else:
        filtered[d['name']] = {
            'school': [d['school']],
            'cnt': d['data']['frequency']
        }

with open('filtered_data.json', 'w') as f:
    json.dump(filtered, f, indent=4, ensure_ascii=False)
