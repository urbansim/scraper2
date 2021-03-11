from __future__ import division

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning) # removes to_pydatetime() warning for now till we update it

from datetime import datetime as dt
from datetime import timedelta
import logging
import urllib
import unicodecsv as csv
from lxml import html, etree
import requests
import time
import sys
from requests.auth import HTTPProxyAuth
from requests.exceptions import Timeout, ConnectionError, ProxyError
import pandas as pd
import numpy as np
import json
import psycopg2
import shutil
import os
import glob
import subprocess
import unicodedata
import re
    
requests.packages.urllib3.disable_warnings()

# Some defaults, which can be overridden when the class is called

DOMAINS = ['http://sfbay.craigslist.org','http://modesto.craigslist.org/search/apa',
           'http://olympic.craigslist.org/search/apa']


# Craigslist doesn't use time zones in its timestamps, so these cutoffs will be
# interpreted relative to the local time at the listing location. For example, dt.now()
# run from a machine in San Francisco will match listings from 3 hours ago in Boston.
EARLIEST_TS = dt.now() - timedelta(hours=0.25)
LATEST_TS = dt.now()

OUT_DIR = '/home/urbansim_service_account/scraper2/data/'
FNAME_BASE = 'data-'  # filename prefix for saved data
FNAME_TS = True  # append timestamp to filename

S3_UPLOAD = False
S3_BUCKET = 'scraper2'


class RentalListingScraper(object):

    def __init__(
            self,
            domains = DOMAINS,
            earliest_ts = EARLIEST_TS,
            latest_ts = LATEST_TS,
            out_dir = OUT_DIR,
            fname_base = FNAME_BASE,
            fname_ts = FNAME_TS,
            s3_upload = S3_UPLOAD,
            s3_bucket = S3_BUCKET):

        self.domains = domains
        self.earliest_ts = earliest_ts
        self.latest_ts = latest_ts
        self.out_dir = out_dir
        self.fname_base = fname_base
        self.fname_ts = fname_ts
        self.s3_upload = s3_upload
        self.s3_bucket = s3_bucket
        self.ts = fname_ts  # Use timestamp as file id
        self.proxies = {
                            'http': 'http://87783015bbe2d2f900e2f8be352c414a:foo@charityengine.services:20000',
                            'https': 'https://87783015bbe2d2f900e2f8be352c414a:foo@charityengine.services:20000'
                        }

        log_fname = '/home/urbansim_service_account/scraper2/logs/' + self.fname_base \
                + (self.ts if self.fname_ts else '') + '.log'
        logging.basicConfig(filename=log_fname, level=logging.DEBUG)

        # Suppress info messages from the 'requests' library
        logging.getLogger('requests').setLevel(logging.WARNING)


    def _get_str(self, list):
        '''
        The xpath() function returns a list of items that may be empty. Most of the time,
        we want the first of any strings that match the xml query. This helper function
        returns that string, or null if the list is empty.
        '''

        if len(list) > 0:
            return list[0]

        return ''


    def _get_int_prefix(self, str, label):
        '''
        Bedrooms and square footage have the format "xx 1br xx 450ft xx". This helper
        function extracts relevant integers from strings of this format.
        '''

        for s in str.split(' '):
            if label in s:
                return s.strip(label)

        return 0


    def _rentToFloat(string_value):
        string_value = string_value.strip()
        try:
            string_value = string_value.decode('utf8')

        except:
            string_value = string_value.decode('unicode-escape')

        string_value = unicodedata.normalize('NFKD',unicode(string_value))
        string_value = string_value.strip()
        string_value = re.sub('[,. ]', '', string_value)    

        try:
            return np.float(string_value) if string_value else np.nan
        except:
            return np.nan

    def _toFloat(self, string_value):
        string_value = string_value.strip()

        try:
            return np.float(string_value) if string_value else np.nan
        except:
            return np.nan

    def _parseListing(self, item):
        '''
        Note that xpath() returns a list with elements of varying types depending on the
        query results: xml objects, strings, etc.
        '''
        try:
            pid = item.xpath('@data-pid')[0]  # post id, always present
            info = item.xpath('div[@class="result-info"]')[0]
            dt = info.xpath('time/@datetime')[0]
            url = item.xpath('a/@href')[0]
            title_raw = info.xpath('h3/a/text()')
            if type(title_raw) == str:
                title = title_raw
            else:
                title = title_raw[0]
            price = self._get_str(info.xpath('span[@class="result-meta"]/span[@class="result-price"]/text()')).strip('$')
            neighb_raw = info.xpath('span[@class="result-meta"]/span[@class="result-hood"]/text()')
            if len(neighb_raw) == 0:
                neighb = ''
            else:
                neighb = neighb_raw[0].strip(" ").strip("(").strip(")")
            housing_raw = info.xpath('span[@class="result-meta"]/span[@class="housing"]/text()')
            if len(housing_raw) == 0:
                beds = 0
                sqft = 0
            else:
                bedsqft = housing_raw[0]
                beds = self._get_int_prefix(bedsqft, "br")  # appears as "1br" to "8br" or missing
                sqft = self._get_int_prefix(bedsqft, "ft")  # appears as "000ft" or missing

            return [pid, dt, url, title, price, neighb, beds, sqft]
        except etree.XMLSyntaxError, e:
            logging.warning('XMLSyntaxError parsing main listing data at {0}: {1}'.format(url, str(e)))
            return None


    def _parseAddress(self, tree):
        '''
        Some listings include an address, but we have to parse it out of an encoded
        Google Maps url.
        '''
        try:
            url = self._get_str(tree.xpath('//p[@class="mapaddress"]/small/a/@href'))
        except etree.XMLSyntaxError, e:
            logging.debug('XMLSyntaxError at parsing address from map at {0}: {1}'.format(url, str(e)))
            return ''

        if '?q=loc' not in url:
            # That string precedes an address search
            return ''

        return urllib.unquote_plus(url.split('?q=loc')[1]).strip(' :')


    def _scrapeLatLng(self, url, proxy=True):

        proxies = {
                  'http': 'http://87783015bbe2d2f900e2f8be352c414a:foo@charityengine.services:20000',
                  # 'https': 'https://87783015bbe2d2f900e2f8be352c414a:foo@charityengine.services:20000'
                }
        try:
            page = requests.get(url, timeout=45, proxies=proxies, verify=False) # was 30
        except:
            time.sleep(2)
            try:
                page = requests.get(url, timeout=45, proxies=proxies, verify=False) # was 30
            except ConnectionError, e:
                logging.debug('ConnectionError at {0}: {1}'.format(url, str(e)))
                return 'connection_errors'
            except ProxyError, e:
                logging.debug('ProxyError at {0}: {1}'.format(url, str(e)))
                return 'proxy_errors'
            except etree.XMLSyntaxError, e:
                logging.debug('XMLSyntaxError getting url at {0}: {1}'.format(url, str(e)))
                return 'xml_syntax_errors'
            except Timeout, e:
                logging.debug('Timeout at {0}: {1}'.format(url, str(e)))
                return 'timeout_errors'

        if page.status_code != 200:
            logging.debug('Status Code {0} trying to connect to {1}'.format(url, page.status_code))
            return 'connection_errors'

        try:
            tree = html.fromstring(page.content)
        except etree.XMLSyntaxError, e:
            logging.debug('XMLSyntaxError parsing tree at {0}: {1}'.format(url, str(e)))
            return 'xml_syntax_errors'
        try:
            baths = tree.xpath('//div[@class="mapAndAttrs"]/p[@class="attrgroup"]/span/b')[1].text[:-2]
        except:
            baths = ''

        # Sometimes there's no location info, and no map on the page
        try:
            map = tree.xpath('//div[@id="map"]')
        except etree.XMLSyntaxError, e:
            logging.debug('XMLSyntaxError getting map at {0}: {1}'.format(url, str(e)))
            return 'xml_syntax_errors'

        if len(map) == 0:
            return 'xml_syntax_errors'
        try:
            map = map[0]
            lat = map.xpath('@data-latitude')[0]
            lng = map.xpath('@data-longitude')[0]
            accuracy = map.xpath('@data-accuracy')[0]
            address = self._parseAddress(tree)
            return [baths, lat, lng, accuracy, address]

        except etree.XMLSyntaxError, e:
            logging.debug('XMLSyntaxError getting map attrs at {0}: {1}'.format(url, str(e)))
            return 'xml_syntax_errors'


    def _get_fips(self, row):

            # old api
            # url = 'http://data.fcc.gov/api/block/find?format=json&latitude={}&longitude={}'
            # new api
            #TODO: dont send to api if the record is not in the US!
            url = 'https://geo.fcc.gov/api/census/block/find?latitude={}&longitude={}&format=json'
            request = url.format(row['latitude'], row['longitude'])

            # FCC api is very sensitive to repeat requests so need to handle timeouts
            try:
                response = requests.get(request, timeout=30)
            except:
                time.sleep(2)
                try:
                    response = requests.get(request, timeout=30)
                except ConnectionError, e:
                    logging.debug('ConnectionError at {0}: {1}'.format(request, str(e)))
                    return 'connection_errors'
                except ProxyError, e:
                    logging.debug('ProxyError at {0}: {1}'.format(request, str(e)))
                    return 'proxy_errors'
                except Timeout, e:
                    logging.debug('Timeout at {0}: {1}'.format(request, str(e)))
                    return 'timeout_errors'

            data = response.json()
            return pd.Series({'fips_block':data['Block']['FIPS'], 'state':data['State']['code'], 'county':data['County']['name']})


    def _clean_listings(self, filename):

        converters = {'neighb':str,
              'title':str,
              'price':self._rentToFloat,
              'beds':self._toFloat,
              'baths':self._toFloat,
              'pid':str,
              'dt':str,
              'url':str,
              'sqft':self._toFloat,
              'sourcepage':str,
              'lng':self._toFloat,
              'lat':self._toFloat}

        all_listings = pd.read_csv(filename, converters=converters)

        if len(all_listings) == 0:
            return [], 0, 0, 0
        all_listings = all_listings.rename(columns={'price':'rent', 'dt':'date', 'beds':'bedrooms', 'neighb':'neighborhood',
                                                    'baths':'bathrooms','lng':'longitude', 'lat':'latitude'})
        all_listings['rent_sqft'] = all_listings['rent'] / all_listings['sqft']
        all_listings['date'] = pd.to_datetime(all_listings['date'], format='%Y-%m-%d')
        all_listings['day_of_week'] = all_listings['date'].apply(lambda x: x.weekday())
        # must have 'craigslist.' to catch `.org` in US and `.ca` in canada
        all_listings['region'] = all_listings['url'].str.extract('https://(.*).craigslist.', expand=False)
        unique_listings = pd.DataFrame(all_listings.drop_duplicates(subset='pid', inplace=False))
        thorough_listings = pd.DataFrame(unique_listings)
        thorough_listings = thorough_listings[thorough_listings['rent'] > 0]
        thorough_listings = thorough_listings[thorough_listings['sqft'] > 0]
        if len(thorough_listings) == 0:
            return [], len(all_listings), 0, 0

        geolocated_filtered_listings = pd.DataFrame(thorough_listings)
        geolocated_filtered_listings = geolocated_filtered_listings[pd.notnull(geolocated_filtered_listings['latitude'])]
        geolocated_filtered_listings = geolocated_filtered_listings[pd.notnull(geolocated_filtered_listings['longitude'])]
        cols = ['pid', 'date', 'region', 'neighborhood', 'rent', 'bedrooms', 'sqft', 'rent_sqft', 'bathrooms', 
                'longitude', 'latitude']
        data_output = geolocated_filtered_listings[cols]

        # TO DO: exception handling for fips
        fips = data_output.apply(self._get_fips, axis=1)             
        geocoded = pd.concat([data_output, fips], axis=1)

        return geocoded, len(all_listings), len(thorough_listings), len(geocoded)

    def _write_db(self, dataframe, domain):
        dbname = 'craigslist'
        host='localhost'
        port=5432
        username='cl_user'
        passwd='my-friend-craig'
        conn_str = "dbname={0} user={1} host={2} password={3} port={4}".format(dbname,username,host,passwd,port)
        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()
        num_listings = len(dataframe)
        print("------Inserting {0} listings from {1} into database.------".format(num_listings, domain))
        prob_PIDs = []
        dupes = []
        writes = []
        for i,row in dataframe.iterrows():
            try:
                cur.execute('''INSERT INTO rental_listings
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                                (row['pid'],row['date'].to_datetime(),row['region'],row['neighborhood'],
                                row['rent'],row['bedrooms'],row['sqft'],row['rent_sqft'],
                                row['longitude'],row['latitude'],row['county'],
                                row['fips_block'],row['state'],row['bathrooms']))
                conn.commit()
                writes.append(row['pid'])
            except Exception, e:
                if 'duplicate key value violates unique' in str(e):
                    dupes.append(row['pid'])
                else:
                    prob_PIDs.append(row['pid'])
                conn.rollback()
                
        cur.close()
        conn.close()
        return prob_PIDs, dupes, writes
    
    def run(self, charity_proxy=True):
    
        colnames = ['pid','dt','url','title','price','neighb','beds','sqft', 'baths',
                        'lat','lng','accuracy','address']
        st_time = time.time()

        # Loop over each regional Craigslist URL (only one if processing in parallel)
        for i, domain in enumerate(self.domains):

            msg = None

            # flags for logging
            neverConnected = True
            neverParsed = True

            # counters
            attempt_mainpage_connect = 0
            attempt_mainpage_parse = 0
            page_num = 0
            rows_written = 0
            ts_skipped = 0
            total_listings = 0
            listing_num = 0

            listing_level_error_dict = {
                'main_data_parsing_errors': 0,
                'aux_data_parsing_errors': 0,
                'connection_errors': 0,
                'timeout_errors': 0,
                'proxy_errors': 0,
                'xml_syntax_errors': 0
            }

            regionName = domain.split('//')[1].split('.craigslist')[0]
            regionIsComplete = False
            
            
            search_url = domain
            logging.debug('BEGINNING NEW REGION: {0}'.format(str.upper(regionName)))

            fname = self.out_dir + regionName + '-' \
                + (self.ts if self.fname_ts else '') + '.csv'

            with open(fname, 'wb') as f:
                writer = csv.writer(f)
                writer.writerow(colnames)
                
                # scroll through main listing pages
                while not regionIsComplete:
                    page_num += 1

                    proxies = {
                          'http': 'http://87783015bbe2d2f900e2f8be352c414a:foo@charityengine.services:20000',
                          # 'https': 'https://87783015bbe2d2f900e2f8be352c414a:foo@charityengine.services:20000'
                        }

                    # we get 3 tries to connect to a main page
                    if attempt_mainpage_connect < 2:
                        try:
                            attempt_mainpage_connect += 1
                            page = requests.get(search_url, proxies=proxies, timeout=45, verify=False) # was 30
                        except:
                            time.sleep(2)
                            continue

                    # last (third) try        
                    else:
                        try:
                            attempt_mainpage_connect += 1
                            page = requests.get(search_url, proxies=proxies, timeout=45, verify=False)

                        # if connection fails a third time, try to catch and log the exception,
                        # and break the loop. Only raise a warning if the connection has not
                        # previously made it past this point.
                        except Timeout, e:
                            msg = 'FAILED TO CONNECT TO {0} WITH TIMEOUT AFTER {1} TRIES.'.format(
                                str.upper(regionName), attempt_mainpage_connect)
                            if neverConnected:
                                logging.warning(msg)
                            else: 
                                logging.debug(msg)
                            break
                        except ConnectionError, e:
                            msg = 'FAILED TO CONNECT TO {0} WITH CONNECTION ERROR AFTER {1} TRIES.'.format(
                                str.upper(regionName), attempt_mainpage_connect)
                            if neverConnected:
                                logging.warning(msg)
                            else: 
                                logging.debug(msg)
                            break
                        except ProxyError, e:
                            msg = 'FAILED TO CONNECT TO {0} WITH PROXY ERROR AFTER {1} TRIES.'.format(
                                str.upper(regionName), attempt_mainpage_connect)
                            if neverConnected:
                                logging.warning(msg)
                            else: 
                                logging.debug(msg)
                            break
                        except Exception, e:
                            msg = 'FAILED TO CONNECT TO {0} WITH UNKNOWN ERROR AFTER {1} TRIES: {2}'.format(
                                str.upper(regionName), attempt_mainpage_connect, str(e))
                            if neverConnected:
                                logging.warning(msg)
                            else: 
                                logging.debug(msg)
                            break
                    
                    # at this point, we will need to log more errors so we set this flag to false
                    # and use it to trigger warnings below. If it breaks above, neverConnected is True.
                    neverConnected = False

                    # we get 3 tries to parse a main page
                    if attempt_mainpage_parse < 2:
                        try:
                            attempt_mainpage_parse += 1
                            tree = html.fromstring(page.content)
                            listings = tree.xpath('//li[@class="result-row"]')
                        except:
                            continue

                    else:
                        # last (third) try        
                        try:
                            tree = html.fromstring(page.content)
                            listings = tree.xpath('//li[@class="result-row"]')

                        # if parsing fails a third time, try to catch and log the exception,
                        # and break the loop. Only raise a warning if parsing has not
                        # previously made it past this point.
                        except Exception, e:
                            if neverParsed:
                                msg = 'NEVER PARSED HTML FOR {0}. {1}: {2}'.format(
                                    str.upper(regionName), type(e).__name__, e)
                                logging.warning(msg)

                            else:
                                msg = 'FAILED TO PARSE HTML FOR {0} PAGE {1}. {2}: {3}'.format(
                                    str.upper(regionName), str(page_num), type(e).__name__, e)
                                logging.debug(msg)
                            break

                    # at this point, we will need to log more errors so we set this flag to false
                    # and use it to trigger warnings below. If it breaks above, neverParsed is True.
                    neverParsed = False

                    # if there are no listings on the page, break the loop with an info-level log message
                    if len(listings) == 0:
                        logging.debug('PARSED HTML BUT FOUND 0 LISTINGS FOR {0} AT PAGE {1}'.format(
                            str.upper(regionName), str(page_num)))
                        break

                    total_listings += len(listings)

                    for item in listings:

                        listing_num += 1
                        try:
                            row = self._parseListing(item)

                            if row is None:
                                listing_level_error_dict['main_data_parsing_errors'] += 1
                                continue

                            item_ts = dt.strptime(row[1], '%Y-%m-%d %H:%M')

                        except Exception, e:
                            msg = "Unknown error parsing main listing data at {2}. {0}: {1}.".format(type(e).__name__, e, regionName)
                            logging.warning(msg)
                            listing_level_error_dict['main_data_parsing_errors'] += 1
                            continue
                
                        if (item_ts > self.latest_ts):

                            # Skip this item bc it was posted after end of timestamp cutoff
                            # but keep scrolling back in time
                            ts_skipped += 1
                            continue

                        elif (item_ts < self.earliest_ts):

                            # Break out of loop and move on to the next region
                            msg = 'REACHED TIMESTAMP CUTOFF FOR {0}'.format(str.upper(regionName))
                            logging.debug(msg)
                            regionIsComplete = True
                            break 
                
                        item_url = row[2]

                        # Parse listing page to get lat/lng if no problems with above parsing
                        try:
                            more_row = self._scrapeLatLng(item_url)
                            if type(more_row) == str:
                                # locals()[more_row] += 1  # i'm not sure this little trick is working
                                listing_level_error_dict[more_row] += 1
                            else:
                                writer.writerow(row + more_row)
                                rows_written += 1

                        except Exception, e:

                            # these should almost always get caught by _scrapeLatLng()
                            msg = "Unknown error scraping lat/lon/baths at {2}. {0}: {1}.".format(type(e).__name__, e, item_url)
                            logging.warning(msg)
                            listing_level_error_dict['aux_data_parsing_errors'] += 1
                            continue

                    # if we haven't completed the region, i.e. there may still
                    # be more listings in our time window, look for the next page button
                    if not regionIsComplete:
                        next = tree.xpath('//a[@title="next page"]/@href')

                        if len(next) > 0:

                            if len(next[0]) > 0:
                                search_url = domain.split('/search')[0] + next[0]

                            # if href attribute is empty, there are no more listings
                            else:
                                regionIsComplete = True    
                                logging.debug('REACHED END OF LISTINGS AT {0}'.format(str.upper(regionName)))
                        
                        # if we cant find the next button at all, there is a real problem!
                        else:
                            logging.error('NO NEXT BUTTON FOUND FOR {0}'.format(str.upper(regionName)))
                            break

            
            # never wrote any rows to .csv. this won't break the cleaning
            # but we should still skip the cleaning process and delete the
            # empty .csv. first we try to identify all legitimate reasons
            # for having zero rows and create log messages for cases that
            # would not have been caught above

            if rows_written == 0:

                # if regionIsComplete, either reached the timestamp cutoff
                # or no next button found, both of which are handled above.
                if regionIsComplete:
                    continue

                # never connected to a main page, error should be caught above
                # with FAILED TO CONNECT message
                elif neverConnected:
                    continue

                # never parsed a main page HTML, error should be caught above
                # with FAILED TO PARSE message
                elif neverParsed:
                    continue

                # 0 rows were written and region is not complete even though
                # we connected, parsed a main page, and found listings.
                # This could indicate a major issue
                elif total_listings > 0:

                    # all listings skipped due to timestamp issues. this is
                    # typically due to a timezone issue
                    if ts_skipped == total_listings:
                        logging.error(('100% OF LISTINGS ARE TOO NEW AT {0}. EARLIEST LISTING' +
                            ' ({2}) > RUN CUTOFF ({1}). CHECK THE TIMEZONE!!!').format(
                             str.upper(regionName),
                             str(self.latest_ts.strftime('%H:%M')),
                             str(item_ts)))
                        continue

                    # no new listings in time window of interest
                    elif listing_num == 1:
                        msg = 'NO NEW LISTINGS AT {0}'.format(str.upper(regionName))
                        logging.info(msg)
                        continue

                    # if it's not a timezone issue, it must be a parsing issue. these should
                    # all be getting logged at the INFO level above, but here we log an
                    # error-level message for the entire region with the counts of the error types.
                    else:
                        logging.error(('0 LISTINGS WRITTEN TO CSV AT {0} EVEN THOUGH WE ' +
                            'FOUND {7}: {1} CONNECTION ERRORS, {2} PROXY ERRORS' +
                            ' {3} TIMEOUT ERRORS, {4} XML SYNTAX ERRORS, {5} MAIN PARSING ERRORS,' +
                            ' {6} aux_data_parsing_errors PARSING ERRORS').format(
                            str.upper(regionName), str(listing_level_error_dict['connection_errors']),
                            str(listing_level_error_dict['proxy_errors']),
                            str(listing_level_error_dict['timeout_errors']),
                            str(listing_level_error_dict['xml_syntax_errors']),
                            str(listing_level_error_dict['main_data_parsing_errors']),
                            str(listing_level_error_dict['aux_data_parsing_errors']),
                            total_listings))
                        continue

                # if not never connected and not never parsed and region is not complete
                # but 0 rows were written bc found 0 total listings, this means the while
                # loop always broke before iterating through items (line 462)
                else:
                    logging.warning(('NO LISTINGS WRITTEN TO CSV AT {0} BC ' +
                        'WE FOUND 0 AFTER PARSING MAIN PAGE.').format(str.upper(regionName)))
                    continue

            # if rows were written, we won't skip cleaning but we still want to log errors if there
            # were a significant amount
            else:
                pct_rows_written = round(rows_written / (listing_num - ts_skipped) * 100, 1)
                msg = ('{7} LISTINGS WRITTEN TO CSV ({8}%) AT {0}: {1} CONNECTION ERRORS, {2} PROXY ERRORS,' +
                        ' {3} TIMEOUT ERRORS, {4} XML SYNTAX ERRORS, {5} MAIN PARSING ERRORS,' + 
                        ' {6} AUX PARSING ERRORS').format(
                            str.upper(regionName), str(listing_level_error_dict['connection_errors']),
                            str(listing_level_error_dict['proxy_errors']),
                            str(listing_level_error_dict['timeout_errors']),
                            str(listing_level_error_dict['xml_syntax_errors']),
                            str(listing_level_error_dict['main_data_parsing_errors']),
                            str(listing_level_error_dict['aux_data_parsing_errors']),
                            str(rows_written), str(pct_rows_written))
                if pct_rows_written < 75 and (listing_num - ts_skipped) > 10:
                    logging.warning(msg)
                elif any(listing_level_error_dict.values()):
                    logging.info(msg)

            # process the results!
            cleaned, count_listings, count_thorough, count_geocoded = self._clean_listings(fname)
            num_cleaned = len(cleaned)

            if num_cleaned > 0:
                probs, dupes, writes = self._write_db(cleaned, domain)
                num_probs = len(probs)
                num_dupes = len(dupes)
                num_writes = len(writes)
                assert num_probs + num_dupes + num_writes == num_cleaned 

                if num_dupes == num_cleaned:

                    # no db writes completed bc they were all dupes
                    logging.info('0 {0} PIDS WRITTEN TO DB. 100% ARE DUPLICATES'.format(str.upper(regionName)))

                elif num_writes + num_dupes == num_cleaned:

                    # all db writes completed besides dupes
                    logging.info('{0} {1} PIDS (100%) WRITTEN TO DB.'.format(str(num_writes), str.upper(regionName)) + 
                                 ' {0} scraped, {1} w/ rent/sqft, {2} w/ lat/lon, {3} dupes'.format(
                                    count_listings, count_thorough, count_geocoded, num_dupes))
                
                else:
                    pct_written = round(num_writes / (num_cleaned - num_dupes) * 100, 3)
                    # db writes failed for reasons other than dupes
                    logging.info('{0} {1} PIDS ({2}%) WRITTEN TO DB. THESE PIDS FAILED: '.format(
                        str(num_writes), str.upper(regionName), pct_written) + ', '.join(probs))

            else:

                # problem reading individual region .csv THIS SHOULD NEVER HAPPEN
                if count_listings == 0:
                    logging.error('ERROR AT {0}. {1} ROWS WRITTEN TO {2} BUT NONE READ BY CLEANER.'.format(
                        str.upper(regionName), str(rows_written), fname))

                # read rows from .csv but none of them met cleaning criteria
                else:
                    logging.info('0 {0} PIDS WRITTEN TO DB. PARSED LISTINGS BUT NONE COULD BE CLEANED:'.format(str.upper(regionName)) + 
                                 ' {0} scraped, {1} w/ rent/sqft, {2} w/ lat/lon.'.format(
                                    count_listings, count_thorough, count_geocoded))

        return
