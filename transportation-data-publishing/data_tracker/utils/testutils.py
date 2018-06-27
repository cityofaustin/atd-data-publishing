import os

from config.secrets import *
from config.public import *

from tdutils import logutil
from tdutils import jobutil
from tdutils import emailutil

import arrow




def dummyfunc():
    print("hello world")

def getscriptname():
    return os.path.basename(__file__).replace('.py', '')

def createlogger(scriptname):

    logfile = f'{LOG_DIRECTORY}/{scriptname}'
    logger = logutil.timed_rotating_log(logfile)

    if scriptname == "backup":
        logger.info('START AT {}'.format( arrow.now() ))

    return logger

def runcatch(func, scriptname):

    logger = createlogger(scriptname)

    job = jobutil.Job(
        name=scriptname,
        url=JOB_DB_API_URL,
        source='knack',
        destination='csv',
        auth=JOB_DB_API_TOKEN)

    try:
        results = func(job)

        if results:
            job.result('success')
            logger.info('END AT {}'.format(arrow.now()))

    except Exception as e:

        logger.error(str(e))

        emailutil.send_email(ALERTS_DISTRIBUTION,
                             SCRIPTINFO[scriptname]['subject'],
                             str(e),
                             EMAIL['user'],
                             EMAIL['password'])

        job.result('error', message=str(e))

        raise e









