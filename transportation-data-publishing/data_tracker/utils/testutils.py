import os
import traceback

from config.secrets import *
from config.public import *

from tdutils import logutil
from tdutils import jobutil
from tdutils import emailutil
from tdutils import argutil

import arrow


def getscriptname():
    return os.path.basename(__file__).replace('.py', '')


def cli_args(scriptname):
    arglist = []

    for arg in SCRIPTINFO[scriptname]["arguments"]:
        arglist.append(arg)

    parser = argutil.get_parser(
        '{}.py'.format(str(scriptname)),
        SCRIPTINFO[scriptname]["argdescription"],
        *arglist)

    # '{}.py'
    # format(str(scriptname)),
    # 'Assign detection status to traffic signal based on status of its detectors.',
    # 'app_name'

    args = parser.parse_args()

    argdict = vars(args)

    return argdict

def createlogger(scriptname):

    logfile = f'{LOG_DIRECTORY}/{scriptname}'
    logger = logutil.timed_rotating_log(logfile)

    logger.info('START AT {}'.format(arrow.now()))

    # if scriptname == "backup":
    #
    #     pass
    #
    # elif scriptname == "detection_status_signals":
    #
    #     logger.info('{} signal records updated'.format(results))
    #     logger.info('END AT {}'.format(arrow.now()))

    return logger


def runcatch(func, scriptname):

    #print(scriptname)

    logger = createlogger(scriptname)

    # find out # of required argument
    # Job creation


    job = jobutil.Job(
        name=scriptname,
        url=JOB_DB_API_URL,
        source=SCRIPTINFO[scriptname]["source"],
        destination=SCRIPTINFO[scriptname]["destination"],
        auth=JOB_DB_API_TOKEN)

    #print(type(job))

    try:

        job.start()

        results = func(job)

        if results:

            job.result('success', records_processed=results)

            logger.info(SCRIPTINFO[scriptname]["loggerresult"].format(results))
            logger.info('END AT {}'.format(arrow.now()))

    except Exception as e:

        error_text = traceback.format_exc()
        logger.error(str(error_text))

        emailutil.send_email(ALERTS_DISTRIBUTION,
                             SCRIPTINFO[scriptname]['subject'],
                             str(e),
                             EMAIL['user'],
                             EMAIL['password'])

        job.result('error', message=str(e))

        raise e

# if __name__ == main:







