# system packages
import os
import traceback

# import configuration dictionaries
from config.secrets import *
from config.public import *
from config.knack.config import cfg

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
    """get command line arguments, the get script part could be refactored as a
    seperated function

    Returns: a dictionary contains all arguments from command line imputs.

    Returns:
        dict

    """

    script_parser = argparse.ArgumentParser()
    script_parser.add_argument("script_name")
    script_name, script_arguments = script_parser.parse_known_args()

    prog = script_name.script_name

    script_name = script_name.script_name.replace(".py", "")

    args_dict = {"script_name": script_name}

    if SCRIPTINFO[script_name]["arguments"] is not None:
        description = SCRIPTINFO[script_name]["argdescription"]
        args_name = SCRIPTINFO[script_name]["arguments"]

        # print("args_name", args_name)

        argument_parser = argutil.get_parser(prog, description, *args_name)

        # print(argument_parser)

        args = argument_parser.parse_args(script_arguments)

        # print("args", args)

        args_dict.update(vars(args))

        if "destination" in args_dict:
            args_dict["destination"] = "".join(args_dict["destination"])


    return args_dict


def create_logger(script_name):
    """

    Args:
        script_name (str): script name

    Returns:

    """

    logfile = f'{LOG_DIRECTORY}/{script_name}.log'
    logger = logutil.timed_rotating_log(logfile)

    logger.info('START AT {}'.format(arrow.now()))

    return logger


def dynamic_import(script_name):
    """

    Args:
        script_name (str) : name of the script as a string

    Returns: script content of the specified script

    """
    module_name = "data_tracker.{}".format(script_name)
    # print(module_name)
    script = importlib.import_module(module_name)

    return script


def create_namejob(script_name):
    """

    Args:
        script_name (str):

    Returns: a job class that will be used as a input to the main function
    from each script.

    """

    job = jobutil.Job(
        name=script_name,
        url=JOB_DB_API_URL,
        source=SCRIPTINFO[script_name]["source"],
        destination=SCRIPTINFO[script_name]["destination"],
        auth=JOB_DB_API_TOKEN)

    return job

def get_script_id(**kwargs):
    """

    Args:
        script_name ():
        **kwarg (dict): a dictionary that contains both the command arguments
        and configuration information


    Returns:

    """

    element_list = []

    for element in SCRIPTINFO[script_name]["id_elements"]:
        element_list.append(kwargs[element])

    script_id = '_'.join(map(str, element_list))

    return script_id

def create_idjob(script_name, script_id, destination):
    """

    Args:
        script_name (str):
        script_id (str):

    Returns:

    """


    job = jobutil.Job(
        name=script_id,
        url=JOB_DB_API_URL,
        source=SCRIPTINFO[script_name]["source"],
        destination = destination,
        auth=JOB_DB_API_TOKEN)

    return job


def run_catch(**kwargs):
    """run the specified python script with command line input and config
    data from configuration documents. Maybe it makes more sense to unpack
    this function and just write it in the __name__ = "__main__".

    Args:
        **kwargs (): the combined dictionary with both command line input and
        configuration from the configuration dictionary

    Returns:various from scripts to scripts, typically defined in the main
    function

    """
    script_name = kwargs["script_name"]


    logger = create_logger(script_name)

    script = dynamic_import(script_name)

    if kwargs["scriptid_flag"] is True:
        script_id = get_script_id(**kwargs)
        job = create_idjob(script_name, script_id, kwargs["destination"])
    else:
        job = create_namejob(script_name)
    job.start()

    try:
        results = getattr(script, "main")(job, **kwargs)

        # print("results", results)

        if results:
            job.result('success', records_processed=results)

            logger.info(SCRIPTINFO[script_name]["logger_result"].format(
                results))
            logger.info('END AT {}'.format(arrow.now()))

    except Exception as e:

        error_text = traceback.format_exc()
        logger.error(str(error_text))

        emailutil.send_email(ALERTS_DISTRIBUTION,
                             SCRIPTINFO[script_name]['subject_t'].format(
                                 kwargs[SCRIPTINFO[script_name]['subject_v']]),
                             str(e),
                             EMAIL['user'],
                             EMAIL['password'])

        job.result('error', message=str(e))

        raise e

    return results


if __name__ == "__main__":

    args_dict = cli_args()

    # print(args_dict)

    script_name = args_dict.get("script_name")

    kwargs = dict(SCRIPTINFO[script_name], **args_dict)

    # print(get_script_id(**kwargs))
    #
    # print(kwargs.get("destination"))

    run_catch(**kwargs)

