# transportation-data-publishing

This repo houses ETL scripts for Austin Transportation's data integration projects. They're written in Python. 

## Quick Start

We use Docker and cron to launch scripts in individual containers on a Linux host. Getting things up and running consists of building the Docker image on the host, then running `build.sh` and `deploy.sh` to start the cron jobs.

1. Install [Docker](https://docs.docker.com/) on a Linux machine.

2. Start Docker: `systemctl start docker`

3. Clone this repository to your Docker host: `git clone https://github.com/cityofaustin/transportation-data-publishing`
 
4. Create your `secrets.py` and drop it into `transportation-data-publishing/config` following the template in [fake-secrets.py](https://github.com/cityofaustin/transportation-data-publishing/blob/master/config/fake_secrets.py)

5. If setting up ESB inegration add certificates to `transportation-data-publishing/config/esb`

6. `cd` into the repository and build the Docker image (this will take a few minutes): `docker build -t tdp-py36 -f Dockerfile-tdp-py36 .`

7. Generate the shell scripts and crontab file: `bash build.sh`.

8. **THIS WILL OVERWRITE ANY EXISTING CRONTAB ON YOUR HOST**
To deploy the scripts, run `bash deploy.sh`. This will install a crontab file (`crontab.sh`) to run shell scripts on the schedules defined in `config.py` and establish log rotation on `transportation-data-publshing/logs` as defined in `tdp.logrotate`.

9. If you want to modify the script configuration, edit job schedules in `config.py` as needed, then run `bash build.sh` and `bash deploy.sh` on your host.

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


