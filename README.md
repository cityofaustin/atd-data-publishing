# transportation-data-publishing

This repo houses ETL scripts for Austin Transportation's open data projects. They're written in Python. 

Check out [sig_pub.py](https://github.com/cityofaustin/transportation-data-publishing/blob/master/sig_pub.py) to see the scripts be happy together.

#### ArcGIS Online Helpers (agol_helpers.py)
Query, add, and delete features from an ArcGIS Online Feature Service

#### Data Helpers (data_helpers.py)
Handy bits of code for common ETL tasks, mostly borrowed from Stack Overflow snippets.

#### Socrata Helpers (knack_helpers.py)
Use the Socrata Open Data API to publish #opendata. 

#### Knack Helpers (knack_helpers.py)
Scripts for accessing the [Knack API](http://knack.freshdesk.com/support/solutions/articles/5000444173-working-with-the-api).

#### Email Helpers (email_helpers.py)
Helpers for sending emails with [yagmail](https://github.com/kootenpv/yagmail)

#### KITS Helpers (kits_helpers.py)
Scripts for accessing the KITS SQL database which supports Austin Transportation's Advanced Traffic Management System (ATMS).

#### Fake Secrets (fake_secrets.py)
Reference file for setting up secrets.py

#### GitHub Helpers (github_helpers.py)
Helpers for commiting to GitHub with programmaticaly. Code borrowed from @luqmaan and @openaustin's [Construction Permits](https://github.com/open-austin/construction-permits) project.
