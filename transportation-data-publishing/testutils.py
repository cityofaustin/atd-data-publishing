# system packages
import os
import traceback
# import sys

# import configuration dictionaries
from config.secrets import *
from config.public import *


# needed packages in tdutils
from tdutils import logutil
from tdutils import jobutil
from tdutils import emailutil
from tdutils import argutil

# other packages
import arrow
import importlib
import argparse


def cli_args():
    # get the name of the script using
    # known, unknown = script_parser.parse_known_args

    script_parser = argparse.ArgumentParser()
    script_parser.add_argument("script_name")
    script_name, script_arguments = script_parser.parse_known_args()

    prog = script_name.script_name

    script_name = script_name.script_name.replace(".py", "")

    args_dict = {"script_name": script_name}

    #print("script name", script_name)
    # create the argument parser using config.public dictionary and arg.util
    if SCRIPTINFO[script_name]["arguments"] is not None:
        description = SCRIPTINFO[script_name]["argdescription"]
        args_name = SCRIPTINFO[script_name]["arguments"]

        argument_parser = argutil.get_parser(prog, description, *args_name)

        args = argument_parser.parse_args(script_arguments)

        args_dict.update(vars(args))

    return args_dict


def createlogger(script_name):


    logfile = f'{LOG_DIRECTORY}/{script_name}'
    logger = logutil.timed_rotating_log(logfile)

    logger.info('START AT {}'.format(arrow.now()))

    return logger

def dynamic_import(script_name):
    module_name = "data_tracker.{}".format(script_name)
    script = importlib.import_module(module_name)

    return script

def runcatch(**kwargs):

    script_name = kwargs["script_name"]
    # print(script_name)

    logger = createlogger(script_name)

    # find out # of required argument


    # Job creation

    job = jobutil.Job(
        name=script_name,
        url=JOB_DB_API_URL,
        source=SCRIPTINFO[script_name]["source"],
        destination=SCRIPTINFO[script_name]["destination"],
        auth=JOB_DB_API_TOKEN)

    #func_dict = {"functioname": backup.main}
    # print(type(job))
    job.start()

    module_name = "data_tracker.{}".format(script_name)
    script = importlib.import_module(module_name)

    results = getattr(script, "main")(job, **kwargs)

    return results
    #################################################
    # try:
    #
    #     job.start()
    #
    #     results = data_tracker.script_name.main(job)
    #
    #     if results:
    #
    #         job.result('success', records_processed=results)
    #
    #         logger.info(SCRIPTINFO[script_name]["loggerresult"].format(results))
    #         logger.info('END AT {}'.format(arrow.now()))
    #
    # except Exception as e:
    #
    #     error_text = traceback.format_exc()
    #     logger.error(str(error_text))
    #
    #     emailutil.send_email(ALERTS_DISTRIBUTION,
    #                          SCRIPTINFO[script_name]['subject'],
    #                          str(e),
    #                          EMAIL['user'],
    #                          EMAIL['password'])
    #
    #     job.result('error', message=str(e))
    #
    #     raise e
    ###############################################################


if __name__ == "__main__":
    # call cli_args function to gather all required arguments

    args_dict = cli_args()

    kwargs = dict(SCRIPTINFO[args_dict["script_name"]], **args_dict)

    runcatch(**kwargs)

    # get the script name

    # print(script_name)
    # call the main in the script associated with the script name, try,
    # catch error, send email
