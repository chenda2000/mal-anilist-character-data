import csv
import argparse
import time
import requests
from html import unescape

parser = argparse.ArgumentParser(description='REQUIRES EXISTING CSV FROM MAL. Because MAL and AniList share IDs for characters, we can add AniList\'s age, description, etc. data to an existing dataset from MAL. AniList contains the mostly the same data as MAL, but it is labelled, unlike MAL which would require some NLP.', epilog='Example run I used: py AddAnilistInfo.py -i data/data.csv io data/dataMod.csv')
parser.add_argument('-w', '--wait', type=float, default=0.7, help='AniList API currently limits requests to 90 per minute, therefore this script will wait by default 0.7 seconds (60 seconds / 90 requests with some leeway) between requests. Decrease at your own risk. For more information: https://anilist.gitbook.io/anilist-apiv2-docs/overview/rate-limiting')
parser.add_argument('-i', '--input', default='data/data.csv', help='File name or path read write data from.')
parser.add_argument('-o', '--output', default='data/dataMod.csv', help='File name or path to write modified data to.')
args = parser.parse_args()

url = 'https://graphql.anilist.co'
query = '''
query ($id: Int) {
  Character(id: $id) {
    description
    gender
    dateOfBirth {
      year
      month
      day
    }
    age
    bloodType
  }
}
'''

def wait():
    time.sleep(args.wait)

def wrapQuotes(string):
    return f'"{string}"'

def run():
    startPerfCounter = time.perf_counter()
    startProcessTime = time.process_time()
    successes = 0

    with open(args.output, 'w', encoding='utf-8-sig') as output:
        output.write('id,name,url,favorites,mostPopularEntry,mpeURL,mpeMembers,mpeType,mpeSource,gender,dateOfBirth,age,bloodType,description\n')

        with open(args.input, 'r', encoding='utf-8-sig') as input:
            reader = csv.reader(input)
            next(reader)
            for row in reader:
                id = { 'id': row[0] }
                response = requests.post(url, json={'query': query, 'variables': id})
                wait()

                data = response.json()['data']
                character = data['Character']
                gender, dateOfBirth, age, bloodType, description = '', '//', '', '', ''

                if character != None:
                    successes += 1

                    gender = character['gender'] or ''
                    date = character['dateOfBirth']
                    dateOfBirth = f'{date["month"] or ""}/{date["day"] or ""}/{date["year"] or ""}'
                    age = character['age'] or ''
                    bloodType = character['bloodType'] or ''
                    desc = unescape(character['description']) or ''
                    description = desc.replace('"', '\'\'').replace('\n', '')

                output.write(f'{",".join(row)},{gender},{dateOfBirth},{wrapQuotes(age)},{bloodType},{wrapQuotes(description)}\n')

    endProcessTime = time.process_time()
    endPerfCounter = time.perf_counter()
    print(f'{successes} successful requests')
    print(f'perf counter: {endPerfCounter - startPerfCounter} seconds, process time: {endProcessTime - startProcessTime} seconds.')

run()