from datetime import datetime as dt
from datetime import timedelta
import time
import os
import multiprocessing
import zipfile
# import subprocess
import glob
import numpy as np
import json
from scraper2 import scraper2
from slackclient import SlackClient
import boto


domains = []

# For testing only comment out these lines
with open('/home/urbansim_service_account/scraper2/domains.txt', 'rb') as f:
    for line in f.readlines():
        domains.append((line.strip()))

# For testing
# domains = ['https://santabarbara.craigslist.org/search/apa']
# domains = ['https://dothan.craigslist.org/search/apa', 'https://santabarbara.craigslist.org/search/apa']
# domains = ['https://micronesia.craigslist.org/search/apa']
# domains = ['http://swv.craigslist.org/search/apa']
# domains = ['http://ashtabula.craigslist.org/search/apa']
# domains = ['http://saltlakecity.craigslist.org/search/apa']

lookback = 1  # hours
earliest_ts = dt.now() - timedelta(hours=lookback)
latest_ts = dt.now() + timedelta(hours=0)
ts = dt.now().strftime('%Y%m%d-%H%M%S')

jobs = []

st_time = time.time()
for domain in domains:
    s = scraper2.RentalListingScraper(
        domains=[domain],
        earliest_ts=earliest_ts,
        latest_ts=latest_ts,
        fname_ts=ts)
    print 'Starting process for ' + domain
    p = multiprocessing.Process(target=s.run)
    jobs.append(p)
    p.start()

for i, job in enumerate(jobs):
    job.join()
    # end_time = time.time()
    # elapsed_time = end_time - st_time
    # time_per_domain = elapsed_time / (i + 1.0)
    # num_domains = len(jobs)
    # domains_left = num_domains - (i + 1.0)
    # time_left = domains_left * time_per_domain
    # print("Took {0} seconds for {1} regions.".format(elapsed_time, i + 1))
    # print("About {0} seconds left.".format(time_left))

# archive the data and delete the raw files
print("Archiving data.")
filepath = '/home/urbansim_service_account/scraper2/archives/'
archive_name = 'rental_listings-' + ts + '.zip'

file_matcher = glob.glob(
    '/home/urbansim_service_account/scraper2/data/*' + str(ts) + '*')
files_to_archive = [fname for fname in file_matcher]

with zipfile.ZipFile(filepath + archive_name, 'w') as zipMe:
    for file in files_to_archive:
        zipMe.write(file, compress_type=zipfile.ZIP_DEFLATED)

archiveSize = np.round(os.path.getsize(
    filepath + archive_name) / 1e3, 2)

token_dict = json.load(open(
    '/home/urbansim_service_account/scraper2/slack_token.json', 'rb'))
slack_token = token_dict['slack']
sc = SlackClient(slack_token)

sc.api_call(
    "chat.postMessage",
    channel="#craigslist-scraper",
    text="Archive created: {0} -- {1} KB".format(
        archive_name + '.zip', archiveSize))

# only files created during a given scraping session will be removed
# which means the data directory may need to be cleaned out from time
# to time.
[os.remove(x) for x in file_matcher]

# replacement for above script that uses s3 instead
archive_path = '/home/urbansim_service_account/scraper2/archives'
conn = boto.connect_s3()
bucket = conn.get_bucket('urbansim-scrapers')

# send any and all archives to the backup location
for item in os.listdir(archive_path):
    local_data_path = '{}/{}'.format(archive_path, item)
    key_name = '{}/archives/{}'.format('craigslist-backups', item)
    print ('Loading {} to S3 at {}.'.format(local_data_path, key_name))
    key = bucket.new_key(key_name)
    key.set_contents_from_filename(local_data_path)

# remove all archives after backing them up
[os.remove(x) for x in glob.glob("{}/*".format(archive_path))]

print("Done.")
