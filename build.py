'''
Generate shell scripts and crontab for deployment of 
transportation-data-publishing scripts.
'''
import os
import sys

from config import CONFIG
from config import DOCKER_BASE_CMD
from config import LOGROTATE


def checkVersion():
    '''
    Check system python version and raise exception if <2.7
    '''
    if sys.version_info[0] < 2:
        raise Exception('Python v2.7+ is required')

    elif sys.version_info[0] == 2 and sys.version_info[1] < 7:
        raise Exception('Python v2.7+ is required')

    return


def shellScript(build_path, path, script, args, image):
    '''
    Build a shell script which will launch python script in Docker container
    '''
    cmd = 'cd {}; python {} {}'.format(
        path,
        script,
        ' '.join(args)
    )

    return DOCKER_BASE_CMD.replace('$BUILD_PATH', build_path).replace('$IMAGE', image).replace('$CMD', cmd)

    
def cronEntry(cron, path):
    '''
    Build a crontab entry
    '''
    return '{} bash {}'.format(cron, path)


def listToFile(list_, filename, write_mode='a+'):
    '''
    Write a list of strings to file
    '''
    with open(filename, write_mode) as fout:
        for l in list_:
            fout.write(l)
            fout.write('\n')
    return


if __name__ == '__main__':
    checkVersion()

    crontab_filename = 'crontab.sh'
    logrotate_filename = 'tdp.logrotate'

    #  Get the absolute path of the repository
    build_path = os.getcwd()

    crons = []
    crons.append('') #  ensures newline when appending to crontab

    for script in CONFIG['scripts']:

        #  ignore script if not enabled
        if not script['enabled']:
            continue

        #  use default image if none is specified
        if not script.get('image'):
            script['image'] = CONFIG['default_image']

        #  generate shell script and write to /shell_scripts
        sh = shellScript(
            build_path,
            script['path'],
            script['script'],
            script['args'],
            script['image']
        )
        
        sh_filename = '{}/transportation-data-publishing/shell_scripts/{}.sh'.format(
            build_path,
            script['name']
        )

        listToFile(
            [sh],
            sh_filename,
            write_mode='w+'
        )

        cron = cronEntry(
            script['cron'],
            sh_filename
        )

        crons.append(cron)

    #  Write cron jobs
    crons.append('') #  ensures last line of crontab file is empty (as required)
    listToFile(
        crons, 
        crontab_filename, 
        write_mode='a+'
    )

    #  Write logrotate config
    logrotate = LOGROTATE.replace('$BUILD_PATH', build_path)
    listToFile(
        [logrotate], 
        logrotate_filename, 
        write_mode='a+'
    )












