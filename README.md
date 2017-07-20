# transportation-data-publishing

This repo houses ETL scripts for Austin Transportation's open data projects. They're written in Python. 

## Quick Start

We use [Anacaonda](https://conda.io/) (actually, [Miniconda](https://conda.io/miniconda.html)) to manage Python environments. If you don't want to use Anaconda, [requirements.txt]() identifies all of the packages required to run the scripts in this repo.

1. Install [Miniconda](https://conda.io/miniconda.html). Check out the [test drive](https://conda.io/docs/test-drive.html#managing-environments) if you haven't used Anaconda before.

2. Clone this repository into your directory of choice: `git clone https://github.com/cityofaustin/transportation-data-publishing`

3. `cd` into the repo directory, and run `conda create --name datapub1 --file requirements.txt` to create the data publishing environment

4. Create your `secrets.py` file following the template in [fake-secrets.py](https://github.com/cityofaustin/transportation-data-publishing/blob/master/fake_secrets.py)

## About the Scripts

#### [ArcGIS Online Helpers](https://github.com/cityofaustin/transportation-data-publishing/blob/master/agol_helpers.py)
Query, add, and delete features from an ArcGIS Online Feature Service

#### [Data Helpers](https://github.com/cityofaustin/transportation-data-publishing/blob/master/data_helpers.py)
Handy bits of code for common ETL tasks, mostly borrowed from Stack Overflow snippets.

#### [Socrata Helpers](https://github.com/cityofaustin/transportation-data-publishing/blob/master/socrata_helpers.py)
Use the Socrata Open Data API to publish #opendata. 

#### [Knack Helpers](https://github.com/cityofaustin/transportation-data-publishing/blob/master/knack_helpers.py)
Scripts for accessing the [Knack API](http://knack.freshdesk.com/support/solutions/articles/5000444173-working-with-the-api).

#### [Email Helpers](https://github.com/cityofaustin/transportation-data-publishing/blob/master/email_helpers.py)
Helpers for sending emails with [yagmail](https://github.com/kootenpv/yagmail)

#### [KITS Helpers](https://github.com/cityofaustin/transportation-data-publishing/blob/master/kits_helpers.py)
Scripts for accessing the KITS SQL database which supports Austin Transportation's Advanced Traffic Management System (ATMS).

#### [GitHub Helpers](https://github.com/cityofaustin/transportation-data-publishing/blob/master/github_helpers.py)
Helpers for commiting to GitHub with programmaticaly. Code borrowed from @luqmaan and @openaustin's [Construction Permits](https://github.com/open-austin/construction-permits) project.

## Contributing

Public contributions are welcome! Assign pull requests to [@johnclary](http://github.com/johnclary).