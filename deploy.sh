#!/bin/bash
#  Deploy crontab and logrotate to Docker host
#  See http://github.com/cityofaustin/transportation-data-publishing

#  deploy crontab
crontab < crontab.sh