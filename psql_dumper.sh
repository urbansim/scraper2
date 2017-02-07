#!/bin/bash

pg_dump -d craigslist -U mgardner | gzip > /home/mgardner/scraper2/psql_dumps/rental_listings-"$(date +%Y%m%d)".gz;

lftp -c 'open -e "set ftps:initial-prot ""; \
   set ftp:ssl-force true; \
   set ftp:ssl-protect-data true; \
   open ftps://ftp.box.com:990; \
   user magardner@berkeley.edu H3xWAma\!LiZEJQ6; \
   mirror --reverse --include-glob=*.gz --Remove-source-files --no-perms --verbose "/home/mgardner/scraper2/psql_dumps" craigslist_psql_dumps;" '
