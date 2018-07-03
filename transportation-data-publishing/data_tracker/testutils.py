import os
import traceback
import sys

# from data_tracker import test

from config.secrets import *
from config.public import *

import argparse

from tdutils import logutil
from tdutils import jobutil
from tdutils import emailutil
from tdutils import argutil

import arrow


def cli_args():

    # get the name of the script using
    # known, unknown = script_parser.parse_known_args

    script_parser = argparse.ArgumentParser()
    script_parser.add_argument("script_name")
    script_name, script_arguments = script_parser.parse_known_args()

    prog = script_name.script_name

    script_name = script_name.script_name.replace(".py", "")

    print("script name", script_name)
    # create the argument parser using config.public dictionary and arg.util
    if SCRIPTINFO[script_name]["argdescription"] is not None:
        description = SCRIPTINFO[script_name]["argdescription"]
        args_name = SCRIPTINFO[script_name]["arguments"]

    argument_parser = argutil.get_parser(prog, description, *args_name)

    args = argument_parser.parse_args(script_arguments)

    args_dict = vars(args)

    args_dict["script_name"] = script_name

    return args_dict

def createlogger(script_name):

    logfile = f'{LOG_DIRECTORY}/{script_name}'
    logger = logutil.timed_rotating_log(logfile)

    logger.info('START AT {}'.format(arrow.now()))

    return logger

def runcatch(func, script_name):

    #print(script_name)

    logger = createlogger(script_name)

    # find out # of required argument


    # Job creation

    job = jobutil.Job(
        name=script_name,
        url=JOB_DB_API_URL,
        source=SCRIPTINFO[script_name]["source"],
        destination=SCRIPTINFO[script_name]["destination"],
        auth=JOB_DB_API_TOKEN)

    #print(type(job))

    try:

        job.start()

        results = func(job)

        if results:

            job.result('success', records_processed=results)

            logger.info(SCRIPTINFO[script_name]["loggerresult"].format(results))
            logger.info('END AT {}'.format(arrow.now()))

    except Exception as e:

        error_text = traceback.format_exc()
        logger.error(str(error_text))

        emailutil.send_email(ALERTS_DISTRIBUTION,
                             SCRIPTINFO[script_name]['subject'],
                             str(e),
                             EMAIL['user'],
                             EMAIL['password'])

        job.result('error', message=str(e))

        raise e

if __name__ == "__main__":

    args_dict = cli_args()

    print("argument_dictionary", args_dict)
    # get the script name
    # call the main in the script associated with the script name, try,
    # catch error, send email