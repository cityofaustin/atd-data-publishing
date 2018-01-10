'''
Get Application Metadata and Write to File
'''
import argparse
import pdb

from fulcrum import Fulcrum
from knackpy import Knack

import _setpath
from config.config import *
from config.secrets import *
from util import fulcutil


def cli_args():
    parser = argparse.ArgumentParser(
        prog='Metadata Getter',
        description='Get metadata from various sources.'
    )

    parser.add_argument(
        'app',
        type=str,
        choices=['knack', 'fulcrum', 'socrata'],
        help="Name of the application that we be accessed."
    )

    args = parser.parse_args()
    
    return(args)


def main(app):
    if app == 'fulcrum':


if __name__ == '__main__':
    args = cli_args()
    app = args.app    
    main(app)










