# appends parent directory to path
# used to just append '..' to path, but that breaks when
# script is imoporte by an external package
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
