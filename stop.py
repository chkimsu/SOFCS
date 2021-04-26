import os
import config.file_path as fp
import utils.marker as mk
import glob
import argparse

parser = argparse.ArgumentParser(description='CDR output handler module.')

# [*]Mandatory parameter.
parser.add_argument('--id', type=str, help='ID of ML processor', required=True)

args = parser.parse_args()

IDX = args.id
fp.IDX = IDX

running_files = glob.glob(fp.run_dir() + "*.run")
for f in running_files:
    with open(f, "r") as file:
        pid = file.readline()
    os.system('kill {}'.format(pid))
    mk.debug_info("Process '{}' is successfully terminated.".format(pid))
