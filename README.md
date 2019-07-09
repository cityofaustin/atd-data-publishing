# transportation-data-publishing

This repo houses ETL scripts for Austin Transportation's data integration projects. They're written in Python. 

## Quick Start

1. Clone this repository to your host: `git clone https://github.com/cityofaustin/transportation-data-publishing`
 
2. Create your `secrets.py` and drop it into `transportation-data-publishing/config` following the template in [fake-secrets.py](https://github.com/cityofaustin/transportation-data-publishing/blob/master/config/fake_secrets.py)

3. If setting up ESB inegration add certificates to `transportation-data-publishing/config/esb`

4. Run scripts as needed, or deploy to a Docker host with [transportation-data-deploy](http://github.com/cityofaustin/transportation-data-deploy)

## About the Repo Structure

#### [bcycle](https://github.com/cityofaustin/transportation-data-publishing/tree/master/transportation-data-publishing/bcycle)

These scripts load B-Cycle tripe data from an Austin B-Cycle Dropbox folder to [data.austintexas.gov](http://data.austintexas.gov).

#### [config](https://github.com/cityofaustin/transportation-data-publishing/tree/master/transportation-data-publishing/config)

Config holds configuration files needed for the various scripts. `secrets.py` belongs here -- see `fake_secrets.py` as a reference.

#### [data_tracker](https://github.com/cityofaustin/transportation-data-publishing/tree/master/transportation-data-publishing/data_tracker)

These scripts modify data in our Data Tracker application, and support its integration with other applications.

#### [open_data](https://github.com/cityofaustin/transportation-data-publishing/tree/master/transportation-data-publishing/open_data)

These scripts publish transportation data to [data.austintexas.gov](http://data.austintexas.gov) and the City's ArcGIS Online organization site.

#### [traffic_study](https://github.com/cityofaustin/transportation-data-publishing/tree/master/transportation-data-publishing/traffic_study)

These are the dedicated files for publishing traffic study data, as described [in the wiki](https://github.com/cityofaustin/transportation-data-publishing/wiki/Traffic-Count-Data-Publishing).

## Contributing

Public contributions are welcome! Assign pull requests to [@johnclary](http://github.com/johnclary).

## Deployment Pipeline

We are currently using CircleCI to automatically rebuild the container on every update to this repo, and autonomously deploy that image to Docker-hub. When working in the production or master branches, there are some considerations to keep in mind:

- You will need someone to review your changes for Production or Master branches.
- The production branch is tagged as `latest` in dockerhub, which means it will be the default image and "source of truth".
- Ideally you will want to merge to master, which will cause CircleCI to create a `master` tag in dockerhub, which can be used for additional testing or troubleshooting.
- Any code merged to production, should be production-worthy already. Test thoroughly in master or any other branch before merging in any additional code.

### Development

Feel free to create a new branch, and commit/push as many times as you need. The pipeline will create a new docker image (if it does not exist), or update the existing image in dockerhub. *Your branch name will be used as the identifying tag in docker hub.*

**For example:**

Say you create a branch with the name `123-atd-updatedcode`, as soon as the branch is created CircleCI will begin building the docker image, and tag it as `atddocker/atd-data-publishing:123-atd-updatedcode` , then it will upload it to Docker hub. If there is already an image with that tag, it will simply update it.



## License

As a work of the City of Austin, this project is in the public domain within the United States.

Additionally, we waive copyright and related rights in the work worldwide through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/).


