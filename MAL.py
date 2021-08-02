from jikanpy import Jikan, exceptions
import time
import argparse
import shelve

jikan = Jikan()
characterRequests = 0
animeRequests = 0
mangaRequests = 0
totalRequests = 0
cacheReads = 0

parser = argparse.ArgumentParser(epilog='Example of run I used: py MAL.py -w 4 -a -p -r 100 200 (Wait 4 seconds between requests, append to existing output file, use persistent cache, IDs ranging from 100 to 200)')
rangeArg = parser.add_mutually_exclusive_group(required=True)
rangeArg.add_argument('-c', '--complete', action='store_true', help='WARNING: THIS WILL TAKE A LONG TIME. I RECOMMEND USING -r AND -a TO BREAK UP BULK REQUESTS. Add this flag to request all character IDs from 1 to 200000. As of July 2021, the highest character IDs are around 198000 (with 138296 total characters), thus 200000. This limit will have to be manually increased in the future as more characters are added to MAL.')
rangeArg.add_argument('-r', '--range', type=int, nargs=2, help='REQUIRED (or use -c). Designate a range of character IDs to request (inclusive). Use with -a to request in smaller batches. Example: \'-r 50 100\'')
parser.add_argument('-w', '--wait', type=int, required=True, help='REQUIRED. Choose a time in seconds to wait between requests. Jikan only allows 2 requests per second (0.5 seconds per request, any lower is useless), but requires 4 seconds per request for bulk requests. Use at your own risk. For more information: https://jikan.docs.apiary.io/#introduction/information/rate-limiting')
parser.add_argument('-a', '--append', action='store_true', help='Add this flag to append to an existing file. Use with -r to request in smaller batches.')
parser.add_argument('-o', '--output', default='data/data.csv', help='File name or path to write data to.')
parser.add_argument('-p', '--persist', action='store_true', help='Add this flag to use a cache that persists across runs. Useful for bulk requests, but data won\'t be guaranteed to be fresh. Delete MALCache file(s) to reset.')
args = parser.parse_args()

def wait():
    time.sleep(args.wait)

def increaseCount(type):
    if type == 'p':
        global cacheReads
        cacheReads += 1
        return

    global totalRequests
    totalRequests += 1
    if type == 'c':
        global characterRequests
        characterRequests += 1
    elif type == 'a':
        global animeRequests
        animeRequests += 1
    elif type == 'm':
        global mangaRequests
        mangaRequests += 1

def getMostPopularEntry(animeography, mangaography, cache):
    mpeID = None
    mpeURL = None
    mpeMembers = 0
    isAnime = True

    for a in animeography:
        # url as key because name and mal_id are not unique across anime/manga
        if a['url'] in cache:
            members = cache[a['url']]['members']
            increaseCount('p')
        else:
            wait()
            members = jikan.anime(a['mal_id'], 'stats')['total']
            increaseCount('a')
            cache[a['url']] = { 'members': members }
        if members > mpeMembers:
            mpeID = a['mal_id']
            mpeURL = a['url']
            mpeMembers = members

    for m in mangaography:
        if m['url'] in cache:
            members = cache[m['url']]['members']
            increaseCount('p')
        else:
            wait()
            members = jikan.manga(m['mal_id'], 'stats')['total']
            increaseCount('m')
            cache[m['url']] = { 'members': members }
        if members > mpeMembers:
            mpeID = m['mal_id']
            mpeURL = m['url']
            mpeMembers = members
            isAnime = False
    return mpeID, mpeURL, isAnime

def write(output, id, name, url, favorites, mostPopularEntry, mpeURL, mpeMembers, mpeType, mpeSource):
    output.write(f'{id},{name},{url},{favorites},{mostPopularEntry},{mpeURL},{mpeMembers},{mpeType},{mpeSource}\n')

def crawl(lower, upper):
    if args.append:
        output = open(args.output, 'a', encoding='utf-8')
    else:
        output = open(args.output, 'w', encoding='utf-8')
        output.write('id,name,url,favorites,mostPopularEntry,mpeURL,mpeMembers,mpeType,mpeSource\n')

    if args.persist:
        cache = shelve.open('MALCache')
    else:
        cache = {}

    for i in range(lower, upper + 1):
        if i % ((upper + 1 - lower) // 10) == 0:
            print(f'{round((i - lower) / (upper + 1 - lower) * 100)}% done.')
        try:
            wait()
            char = jikan.character(i)
            increaseCount('c')
            id, name, url, favorites = char['mal_id'], char['name'], char['url'], char['member_favorites']
            animeography, mangaography = char['animeography'], char['mangaography']

            mpeID, mpeURL, isAnime = getMostPopularEntry(animeography, mangaography, cache)
            
            if mpeID == None or mpeURL == None:
                mostPopularEntry, mpeURL, mpeMembers, mpeType, mpeSource = '', '', '', '', ''
            elif mpeURL in cache and len(cache[mpeURL]) > 1:
                mpe = cache[mpeURL]
                increaseCount('p')
                mostPopularEntry, mpeMembers, mpeType, mpeSource = mpe['title'], mpe['members'], mpe['type'], mpe['source']
            else:
                wait()
                if isAnime:
                    mpe = jikan.anime(mpeID)
                    increaseCount('a')
                    mpeSource = mpe['source']
                else:
                    mpe = jikan.manga(mpeID)
                    increaseCount('m')
                    mpeSource = ''

                mostPopularEntry, mpeMembers, mpeType = mpe['title'], mpe['members'], mpe['type']
                cache[mpeURL] = {
                    'title': mostPopularEntry,
                    'members': mpeMembers,
                    'type': mpeType,
                    'source': mpeSource,
                }

            write(output, id, name, url, favorites, mostPopularEntry, mpeURL, mpeMembers, mpeType, mpeSource)
        except exceptions.APIException:
            continue

    cache.close()
    output.close()

def run():
    if args.complete:
        lower, upper = 1, 200000
    elif args.range[0] <= args.range[1]:
        lower, upper = args.range[0], args.range[1]
    elif args.range[0] > args.range[1]:
        lower, upper = args.range[1], args.range[0]

    print(f'Runtime estimate: {2 * args.wait * (upper - lower + 1)} seconds.')
    startPerfCounter = time.perf_counter()
    startProcessTime = time.process_time()

    crawl(lower, upper)

    endProcessTime = time.process_time()
    endPerfCounter = time.perf_counter()
    print(f'{totalRequests} requests: {characterRequests} character, {animeRequests} anime, and {mangaRequests} manga. {cacheReads} cached requests.')
    print(f'perf counter: {endPerfCounter - startPerfCounter} seconds, process time: {endProcessTime - startProcessTime} seconds.')

run()

'''
csv schema: id, name, url, favorites, mostPopularEntry, mpeURL, mpeMembers, mpeType, mpeSource 

character request schema (JSON): {
    request_hash, request_cached, request_cache_expiry,
    mal_id,
    url, image_url,
    name, name_kanji, [nicknames],
    about,
    member_favorites,
    [animeography {mal_id, name, url, image_url, role}],
    [mangaography {mal_id, name, url, image_url, role}],
    [voice_actors {mal_id, name, url, image_url, language}],
    jikan_url,
    headers {Server, Date, Content-Type, Content-Length, Connection, Access-Control-Allow-Origin, Access-Control-Allow-Methods, Cache-Control, ETag, X-Request-Hash, X-Request-Cached, X-Request-Cache-Ttl, Expires, Content-Encoding, Vary, X-Cache-Status}
}

anime/manga stats request schema (JSON): {
    request_hash, request_cached, request_cache_expiry,
    watching, completed, on_hold, dropped, plan_to_watch, total, 
    scores {1..10 {votes, percentage}}, 
    jikan_url,
    headers {Server, Date, Content-Type, Content-Length, Connection, Access-Control-Allow-Origin, Access-Control-Allow-Methods, Cache-Control, ETag, X-Request-Hash, X-Request-Cached, X-Request-Cache-Ttl, Expires, Content-Encoding, Vary, X-Cache-Status}
}

anime/manga request schema (JSON): {
    request_hash, request_cached, request_cache_expiry,
    mal_id, 
    url, image_url, trailer_url, 
    title, title_english, title_japanese, title_synonyms, 
    type, 
    source, 
    episodes, 
    status, aired {from, to, prop {from {day, month, year}, to {day, month, year}}, string}, 
    duration, 
    rating, 
    score, scored_by, rank, 
    popularity, members, 
    favorites, 
    synopsis, 
    premiered, 
    broadcast, 
    related {['type' {mal_id, type, name, url}]}, 
    [licensors {mal_id, type, name, url}], 
    [studios {mal_id, type, name, url}], 
    [genres {mal_id, type, name, url}], 
    [opening_themes], [ending_themes], 
    jikan_url,
    headers {Server, Date, Content-Type, Content-Length, Connection, Access-Control-Allow-Origin, Access-Control-Allow-Methods, Cache-Control, ETag, X-Request-Hash, X-Request-Cached, X-Request-Cache-Ttl, Expires, Content-Encoding, Vary, X-Cache-Status}
}
'''
