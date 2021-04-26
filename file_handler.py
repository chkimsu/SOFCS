"""
@ File name: file_handler.py
@ Version: 1.3.6
@ Last update: 2020.JAN.15
@ Author: DH.KIM
@ Company: Ntels Co., Ltd
"""

import pandas as pd
import csv
import glob
import time
import config.file_path as fp
import os
import traceback
import timeit
import utils.marker as mk
import shutil
import argparse

from datetime import datetime, timedelta
from utils.logger import FileLogger
from utils.graceful_killer import GracefulKiller


class Clean(GracefulKiller):
    def exit_gracefully(self, signum, frame):
        os.remove(fp.run_dir() + "file_handler.run")
        mk.debug_info("file_handler running end..")
        self.kill_now = True
        logger.debug("Program killed by signal.")
        # raise SystemExit


def file_handler(in_file):
    """
    Read an original file and convert to trainable file. If finish converting, remove the original file.
    :param in_file: A String. Input file path.
    :return: None
    """
    df = pd.read_csv(in_file, delimiter='|', header=None, names=['PGW_IP', 'DTmm', 'SVC_TYPE', 'UP', 'DN'],
                     dtype={
                         "PGW_IP": str,
                         "DTmm": str,
                         "SVC_TYPE": str,
                         "UP": float,
                         "DN": float
                     })

    # Drop Empty Rows
    elogger.warning("Empty filed data is occurred: \n{}".format(df[df.isnull().any(axis=1)]))
    df = df.dropna()

    ip_addr = df['PGW_IP'].unique().tolist()
    svc_type = df['SVC_TYPE'].unique().tolist()

    # NOTE: For every single IPs.
    for ip in ip_addr:
        # NOTE: For every single services.
        for svc in svc_type:
            output_path = fp.input_dir(ip, svc)

            # NOTE: If output path doesn't exist, create one.
            if not os.path.exists(output_path):
                os.makedirs(output_path)

            selected = df.loc[(df['PGW_IP'] == ip) & (df['SVC_TYPE'] == svc)]

            selected = selected.sort_values(['DTmm']).reset_index(drop=True)
            selected = selected.values.tolist()

            if len(selected) > 0:
                # [*]Output file path
                output_path = output_path + '{}.DAT'.format(datetime.now())

                with open(output_path, 'w') as out:
                    writer = csv.writer(out, delimiter='|')
                    for s in selected:
                        writer.writerow(s)

                with open(output_path + ".INFO", "w") as out:
                    out.write("")

                # [*]Log
                logger.info("Successfully write the file: {}".format(output_path))
                logger.debug("Successfully write the info file: {}".format(output_path + ".INFO"))
                logger.debug("{} :: {}".format(svc, selected))
            else:
                logger.info("Service type doesn't have any data: {}".format(svc))

    # [*]Log
    logger.info("Job is finished: {}".format(in_file))

    # [*]copy the file into backup directory.
    file_name = in_file.split("/")[-1]
    shutil.copyfile(in_file, fp.backup_dir()+file_name)
    logger.debug("{} File is backed up into \'{}\'".format(file_name, fp.backup_dir()))

    # [*]If clearly finished, remove original file.
    os.remove(in_file)
    logger.debug("Files are deleted successfully: {}".format(in_file))


def main():
    global today, tomorrow
    global logger, elogger
    global LOG_LEVEL, ID

    while not killer.kill_now:
        # [*]If file doesn't exist, make one.
        directory_check()
        # [*]If Day pass by create a new log file.
        today = datetime.now().date()
        if today >= tomorrow:
            tomorrow = today + timedelta(days=1)

            # [*]Log handler updates
            update_log_path = fp.log_dir() + 'file_handler_{}.log'.format(today)
            update_elog_path = fp.log_dir() + 'file_handler_error_{}.log'.format(today)

            logger = FileLogger('file_handler_info', log_path=update_log_path, level=LOG_LEVEL).get_instance()
            elogger = FileLogger('file_handler_error', log_path=update_elog_path, level='WARNING').get_instance()

        try:
            # [*]File read & check
            info_list = glob.glob(fp.original_input_path() + '*.INFO')
            if info_list:
                logger.debug("Info files: {}".format(info_list))
                stime = timeit.default_timer()
                file = []
                for il in info_list:
                    # [*]Remove .INFO extension.
                    temp = il[:-5]
                    file.append(temp)
                file = sorted(file)
                logger.debug("Loaded files: {}".format(file))

                for f in file:
                    file_handler(f)

                for il in info_list:
                    # [*]If clearly finished, remove original file.
                    os.remove(il)
                logger.debug("Info files are removed: {}".format(info_list))
                etime = timeit.default_timer()
                logger.info("Main job's running time: {}".format(etime-stime))
            time.sleep(1)
        except Exception:
            # [*]Log the errors.
            elogger.error(traceback.format_exc())
            os.remove(fp.run_dir() + "file_handler.run")
            mk.debug_info("file_handler didn't work properly. Check your error log: {}".format(elog_path))
            raise SystemExit


def directory_check():
    # [*]If file doesn't exist, make one.
    if not os.path.exists(fp.log_dir()):
        os.makedirs(fp.log_dir())
        mk.debug_info("Log dir is not exist create one - {}".format(fp.log_dir()), "INFO")

    if not os.path.exists(fp.run_dir()):
        os.makedirs(fp.run_dir())
        mk.debug_info("running dir is not exist create one - {}".format(fp.run_dir()), "INFO")

    if not os.path.exists(fp.original_input_path()):
        os.makedirs(fp.original_input_path())
        mk.debug_info("INPUT dir is not exist create one - {}".format(fp.original_input_path()), "INFO")

    if not os.path.exists(fp.final_output_path()):
        os.makedirs(fp.final_output_path())
        mk.debug_info("OUTPUT dir is not exist create one - {}".format(fp.final_output_path()), "INFO")

    if not os.path.exists(fp.backup_dir()):
        os.makedirs(fp.backup_dir())
        mk.debug_info("BACKUP dir is not exist create one - {}".format(fp.backup_dir()), "INFO")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='CDR output handler module.')

    # [*]Mandatory parameter.
    parser.add_argument('--id', type=str, help='ID of ML processor', default="main")

    # [*]Hyper parameters.
    parser.add_argument('--log', type=str, help='Set the log level', default="INFO")
    args = parser.parse_args()

    fp.IDX = args.id
    LOG_LEVEL = args.log

    # [*]If file doesn't exist, make one.
    directory_check()

    # [*]Every day logging in different fie.
    today = datetime.now().date()
    tomorrow = today + timedelta(days=1)

    log_path = fp.log_dir() + 'file_handler_{}.log'.format(today)
    elog_path = fp.log_dir() + 'file_handler_error_{}.log'.format(today)

    '''
        - logger; Informative logger.
        - elogger; error logger.
    '''
    logger = FileLogger('file_handler_info', log_path=log_path, level=LOG_LEVEL).get_instance()
    elogger = FileLogger('file_handler_error', log_path=elog_path, level='WARNING').get_instance()

    if os.path.exists(fp.run_dir() + "file_handler.run"):
        elogger.error("File handler is already running. Program exit.")
        mk.debug_info("File handler is already running. Program exit.", m_type="ERROR")
    else:
        with open(fp.run_dir() + "file_handler.run", "w") as run_file:
            run_file.write(str(os.getpid()))

    '''
        Graceful killer
    '''
    killer = Clean()

    mk.debug_info("file_handler start running.")
    main()


