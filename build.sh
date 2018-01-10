#!/bin/bash
#  Builds transportation-data-publshing shell scripts
#  See http://github.com/cityofaustin/transportation-data-publishing
#  requires Python 2.7+

#  remove any shell scripts that happen to be in directory
rm -f transportation-data-publishing/shell_scripts/*.sh

#  build new shell scripts and crontab
python build.py