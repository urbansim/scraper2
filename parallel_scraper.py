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
import pytz.reference
# import requests
# from requests.exceptions import ConnectionError
import logging


domains = []

# For testing only comment out these lines
with open('/home/urbansim_service_account/scraper2/domains.txt', 'rb') as f:
    for line in f.readlines():
        domains.append((line.strip()))

# For testing
# domains = ['http://santabarbara.craigslist.org/search/apa']
# domains = ['http://dothan.craigslist.org/search/apa', 'http://santabarbara.craigslist.org/search/apa']
# domains = ['http://micronesia.craigslist.org/search/apa']
# domains = ['http://swv.craigslist.org/search/apa']
# domains = ['http://ashtabula.craigslist.org/search/apa']
# domains = ['http://honolulu.craigslist.org/search/apa']
# domains = ['http://atlanta.craigslist.org/search/apa']
# domains = ['http://toronto.craigslist.org/search/apa']

lookback = 1  # hours
# need to specify machine time in UTC for conversions below
earliest_ts = dt.now(pytz.utc) - timedelta(hours=lookback)
latest_ts = dt.now(pytz.utc) + timedelta(hours=0)
ts = dt.now().strftime('%Y%m%d-%H%M%S')

jobs = []

## Get timezones for each domain once
# get from stored json copied saved from https://reference.craigslist.org/Areas download May 2018
# use this as we are having connection issues from too many requests
with open('/home/urbansim_service_account/scraper2/craigslist_reference.json') as json_data:
    data = json.load(json_data)
# # get live from craigslist at http://reference.craigslist.org/Areas'
# try:
#     url = 'https://reference.craigslist.org/Areas'
#     data = requests.get(url, timeout=45).json()
#     print 'Successfully connected to {}'.format(url)
# except ConnectionError, e:
#     logging.debug('ConnectionError at {0}: {1}'.format(url, str(e)))
#     print ('ConnectionError at {0}: {1}'.format(url, str(e)))

## Parse into {host:{timezone,country}}
# get domains for US, Canada, Puerto Rico, and Micronesia (Guam)
country_list = ['US', 'CA', 'PR', 'GU']
region_time_zone_dict = {}
for item in data:
    if item['Country'] in country_list:
        region_time_zone_dict.update({item['Hostname']:{'Timezone': item['Timezone'], 'Country': item['Country']}})
logging.info('Found {} domains for countries: {}'.format(len(region_time_zone_dict),country_list))
print ('Found {} domains for countries: {}'.format(len(region_time_zone_dict),country_list))

# Check if host exists in api and visa versa to see if any updates are required to domain list
hostname_list = []
for domain in domains:
    hostname_list.append(domain[7:].split('.')[0]) # assumes http not https

for host in hostname_list:
    if host not in region_time_zone_dict.keys():
        logging.warning('{} host not found in craigslist regions api host list'.format(host))
        print ('{} host not found in craigslist regions api host list'.format(host))

for key, value in region_time_zone_dict.iteritems():
    if key not in hostname_list:
        logging.warning(
            '{} host in craigslist regions api not found in current list of domains host list'.format(key))
        print (
            '{} host in craigslist regions api not found in current list of domains host list'.format(key))
        logging.warning('Consider adding domain: http://{}.craigslist.org/search/apa'.format(key))
        print ('Consider adding domain: http://{}.craigslist.org/search/apa'.format(key))

st_time = time.time()
for domain in domains:

    ## need offset earliest and latest times to deal with domain timezones
    # get time zones for domain
    hostname = domain[7:].split('.')[0] # assumes http not https
    domain_timezone = region_time_zone_dict[hostname]['Timezone']
    machine_tz = pytz.timezone('America/Los_Angeles')
    tz = pytz.timezone(domain_timezone)

    # convert machine time UTC to domain time tz and local machine tz to get offset in UTC for both then subtract them
    earliest_ts_offset_utc = earliest_ts.astimezone(machine_tz).utcoffset() - earliest_ts.astimezone(tz).utcoffset()
    latest_ts_offset_utc = latest_ts.astimezone(machine_tz).utcoffset() - latest_ts.astimezone(tz).utcoffset()

    # subtract the UTC offset to the UTC machine time
    earliest_ts_utc_woffset = earliest_ts - earliest_ts_offset_utc
    latest_ts_utc_woffset = latest_ts - latest_ts_offset_utc

    # convert offset machine time in UTC to domain time in local tz
    earliest_ts_machine_tz_woffset = earliest_ts_utc_woffset.astimezone(machine_tz)
    latest_ts_machine_tz_woffset = latest_ts_utc_woffset.astimezone(machine_tz)

    # print to check times
    fmt = '%Y-%m-%d %H:%M:%S %Z%z'
    print('earliest: machine time in PST is: {}'.format(earliest_ts.astimezone(machine_tz).strftime(fmt)))
    print('earliest: converted to PST to match {}: {}'.format(domain_timezone,
                                                    earliest_ts_utc_woffset.astimezone(machine_tz).strftime(fmt)))
    print('latest: machine time in PST is: {}'.format(latest_ts.astimezone(machine_tz).strftime(fmt)))
    print('latest: converted to PST to match {}: {}'.format(domain_timezone,
                                                            latest_ts_machine_tz_woffset.astimezone(machine_tz).strftime(fmt)))

    # make datetime not tz aware for use in scraper
    earliest_ts_machine_tz_woffset_clean = earliest_ts_machine_tz_woffset.replace(tzinfo=None)
    latest_ts_machine_tz_woffset_clean = latest_ts_machine_tz_woffset.replace(tzinfo=None)

    s = scraper2.RentalListingScraper(
        domains=[domain],
        earliest_ts=earliest_ts_machine_tz_woffset_clean,
        latest_ts=latest_ts_machine_tz_woffset_clean,
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
        archive_name, archiveSize))

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
