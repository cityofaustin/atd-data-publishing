if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import os
import sys
import pdb
import arrow
import kits_helpers
import knack_helpers
import data_helpers
import secrets



query = "SELECT * FROM KITSDB.KITS.CAMERA"


def main(date_time):
    print('starting stuff now')

    kits_data = kits_helpers.data_as_dict(kits_creds, query)


    pdb.set_trace()

if __name__ == '__main__':
    
    now = arrow.now()
    now_s = now.format('YYYY_MM_DD')
    
    #  init logging 
    #  with one logfile per dataset per day
    cur_dir = os.path.dirname(__file__)
    logfile = 'log/{}_{}.log'.format('kits_camera_sync', now_s)
    log_path = os.path.join(cur_dir, logfile)
    logging.basicConfig(filename=log_path, level=logging.INFO)
    logging.info('START AT {}'.format(str(now)))

    kits_creds = secrets.KITS_CREDENTIALS

    maint(now)
    
#  get KITS data
#  get KNACK data
#  detect changes
#  new in KITS
#  missing from KITS
#  delete from KITS (eek!)
#  log results
#  email when camera is created?