#!/bin/bash
#  Deploy crontab and logrotate to Docker host
#  See http://github.com/cityofaustin/transportation-data-publishing

#  deploy crontab
sudo sh -c "crontab -l > tmp"
sudo sh -c "cat crontab.sh >> tmp"
sudo crontab < tmp
sudo rm tmp