# transportation-data-publishing

This repo houses ETL scripts for Austin Transportation's open data projects. They're written in Python. 

## Quick Start

We use [Anaconda](https://conda.io/miniconda.html) to manage Python environments. If you don't want to use Anaconda, skip step #1 and in step #3 use `pip install -r requirements.txt`.

1. Install [Miniconda](https://conda.io/miniconda.html). Check out the [test drive](https://conda.io/docs/test-drive.html#managing-environments) if you haven't used Anaconda before.

2. Clone this repository into your directory of choice: `git clone https://github.com/cityofaustin/transportation-data-publishing`

3. `cd` into the repo directory, and run `conda create --name datapub1 --file requirements.txt` to create the data publishing environment

4. Create your `secrets.py` file following the template in [fake-secrets.py](https://github.com/cityofaustin/transportation-data-publishing/blob/master/config/fake_secrets.py)

## About the Repo Structure

#### [bcycle]()

These scripts load B-Cycle tripe data from an Austin B-Cycle Dropbox folder to [data.austintexas.gov](http://data.austintexas.gov).

#### [config]()

Config holds configuration files needed for the various scripts. `secrets.py` belongs here -- see `fake_secrets.py` as a reference.

#### [data_tracker]()

These scripts modify data in our Data Tracker application, and support its integration with other applications.

#### [open_data]()

These scripts publish transportation data to [data.austintexas.gov](http://data.austintexas.gov) and the City's ArcGIS Online organization site.

#### [shell_scripts]()

This is where we maintain the various shell scripts that instruct our VMs to run our Python code.

#### [traffic_study]()

These are the dedicated files for publishing traffic study data, as described [in the wiki](https://github.com/cityofaustin/transportation-data-publishing/wiki/Traffic-Count-Data-Publishing).

#### [util]()

We maintain general-purpose util scripts in the `util` folder. They store useful routines such as connecting to databases, publishing to specific applications, or converting between date formats.

## Contributing

Public contributions are welcome! Assign pull requests to [@johnclary](http://github.com/johnclary).

## License

As a work of the City of Austin, this project is in the public domain within the United States.

Additionally, we waive copyright and related rights in the work worldwide through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/).


