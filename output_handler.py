"""
@ File name: output_handler.py
@ Version: 1.3.6
@ Last update: 2020.JAN.15
@ Author: DH.KIM
@ Company: Ntels Co., Ltd
"""

import os
import csv
import pandas as pd
import config.file_path as fp
import time
import traceback
import glob
import timeit
import utils.marker as mk
import numpy as np
import argparse

from multiprocessing import Process, Queue
from utils.logger import StreamLogger, FileLogger
from datetime import datetime, timedelta
from utils.graceful_killer import GracefulKiller

STREAM_LOG_LEVEL = "WARNING"


class Clean(GracefulKiller):
    def exit_gracefully(self, signum, frame):
        # [*]Process killed by command or Keyboard Interrupt.
        os.remove(fp.run_dir() + "output_handler.run")
        mk.debug_info("output_handler running end..")
        self.kill_now = True
        logger.debug("Program killed by signal.")


def get_running_process():
    """
    Get running anomaly detection process.
    :return:
        - A dictionary. Dictionary of p-gateway ip and its service ids.
    """
    running_file = glob.glob(fp.run_dir() + "*.detector.run")
    running_process = {}
    for rf in running_file:
        temp = rf.split("/")[-1].split("_")
        pgw_ip = temp[0]
        service = temp[-1].split(".")[0]
        if pgw_ip not in running_process.keys():
            running_process[pgw_ip] = [service]
        else:
            running_process[pgw_ip].append(service)
    logger.debug("Running process - {}".format(running_process))
    return running_process


def multi_process_by_ip(pid, svc_list, mq):
    """
    This method is worked by multi-processing. It gather the output data from each service directory using thread.
    And then write a file.

    :param pid: A String. P-gateway IP.
    :param svc_list: A List. List of working anomaly detector.
    :return: None.
    """
    # [*] Initializing variables.
    files = []
    for svc in svc_list:
        stime = timeit.default_timer()
        info_file = glob.glob(fp.management_dir() + "/{}/{}/output/*.DAT.INFO".format(pid, svc))
        logger.debug("INFO files: {}/{}::{}".format(pid, svc, info_file))
        etime = timeit.default_timer()
        logger.debug(".INFO searched time: {}".format(etime-stime))

        # [*] Remove .INFO extension.
        stime = timeit.default_timer()
        for info in info_file:
            temp = info[:-5]
            files.append(temp)

        etime = timeit.default_timer()
        logger.debug("Extension removal time: {}".format(etime-stime))

    if not files:
        logger.info("There is no data to process in: {}".format(pid))
        return

    all_data = []
    # [*] Integrate all data in files.
    for f in files:
        with open(f, "r") as file:
            csv_reader = csv.reader(file, delimiter="|")
            for line in csv_reader:
                if len(line) < 8:
                    line.append(np.nan)
                all_data.append(line)

    logger.info("Collected data - {}".format(all_data))
    mq.put(all_data)

    # [*] Remove finished files.
    for f in files:
        alpha = f + ".INFO"
        os.remove(f)
        os.remove(alpha)
        logger.debug("file is deleted: {}".format(f))
        logger.debug("info files are deleted: {}".format(alpha))


def directory_check():
    # [*]Make Final output directory, if doesn't exist.
    if not os.path.exists(fp.final_output_path()):
        os.makedirs(fp.final_output_path())
        mk.debug_info("OUTPUT dir doesn't exist create one. - {}".format(fp.final_output_path()))

    if not os.path.exists(fp.run_dir()):
        os.makedirs(fp.run_dir())
        mk.debug_info("running dir doesn't exist create one. - {}".format(fp.run_dir()))

    if not os.path.exists(fp.log_dir()):
        os.makedirs(fp.log_dir())
        mk.debug_info("log dir doesn't exist create one. - {}".format(fp.log_dir()))


def main():
    global today, tomorrow
    global elogger, logger, slogger
    global killer
    global sleep_time
    global LOG_LEVEL, ID

    while not killer.kill_now:
        directory_check()
        today = datetime.now().date()
        # [*]If Day pass by create a new log file.
        if today >= tomorrow:
            today = tomorrow
            tomorrow = today + timedelta(days=1)

            # [*]Log handler updates
            update_elog_path = fp.log_dir() + 'output_handler_error_{}.log'.format(today)
            update_log_path = fp.log_dir() + 'output_handler_{}.log'.format(today)

            elogger = FileLogger("output_handler_error", update_elog_path, level="WARNING").get_instance()
            logger = FileLogger("output_handler", update_log_path, level=LOG_LEVEL).get_instance()

        # [*]Multi-process init.
        multi_process = []

        try:
            # --------------------------------------
            slogger.debug("Running process list up starts.")
            stime = timeit.default_timer()

            # [*]Get all files from management path.
            process_list = get_running_process()

            if not process_list:
                time.sleep(sleep_time)
                continue

            slogger.debug("Got running process: {}".format(process_list))
            etime = timeit.default_timer()
            logger.info("'get_running_process' function required time: {}".format(etime-stime))
            # --------------------------------------

            slogger.debug("Multiprocessing starts.")
            stime = timeit.default_timer()

            # [*] Multi-process Queue.
            q = Queue()

            # [*] Multi-process by ip address.
            for p in process_list.keys():
                process = Process(target=multi_process_by_ip, args=(p, process_list[p], q,), name=p)
                process.start()
                multi_process.append(process)

            logger.info("Multiprocess starts: {}".format(multi_process))

            for mp in multi_process:
                mp.join()

            # [*] Sleep time to join.
            time.sleep(sleep_time)

            # [*] Collect data.
            final_data = []

            while not q.empty():
                final_data += q.get()

            etime = timeit.default_timer()

            logger.info("Multiprocessing require time: {}".format(etime - stime))
            slogger.debug("Multiprocessing ends.")

            # [*] To file.
            if final_data:
                # [*] Integrate date frames from queue.
                df_all_data = pd.DataFrame(final_data, columns=["PGW_IP", "DTmm", "SVC_TYPE", "REAL_UP", "REAL_DN",
                                                                "ANOMALY_SCORE", "ESTIMATION", "PERCENTAGE"])
                # [*] Sort by time and re-indexing.
                if not df_all_data.empty:
                    df_all_data.sort_values(by=["DTmm"], inplace=True)
                    df_all_data = df_all_data.reset_index(drop=True)

                logger.debug("Integrated data - {}\n".format(df_all_data))

                dt = datetime.now()
                dt = dt.strftime("%Y%m%d_%H%M")
                # [*] Write into OUTPUT file.
                output_path = fp.final_output_path() + "POFCSSA.POLICY.{}.DAT.RESULT".format(dt)

                with open(output_path, "w") as file:
                    csv_writer = csv.writer(file, delimiter='|')
                    np_data = df_all_data.to_numpy()
                    for d in np_data:
                        csv_writer.writerow(d)
                logger.info("File is written {}".format(output_path))

                with open(output_path + ".INFO", "w") as file_pointer:
                    file_pointer.write("")
                logger.info("INFO file is written {}".format(output_path + ".INFO"))
        except Exception:
            elogger.error(traceback.format_exc())
            slogger.error("Output handler didn't work properly. Check your error log: {}".format(log_path))
            os.remove(fp.run_dir() + "output_handler.run")
            raise SystemExit


if __name__ == "__main__":
    """
    Work flow:
        1) Log directory create, if doesn't exist.
        2) Logger define.
        3) While roof
            3-1) Get running process of anomaly detection module.
            3-2) Each IP address got it's process to integrate all output data from it's service type.
            3-3) Gathered data is converted into pandas DataFrame and write into a file.
    """
    parser = argparse.ArgumentParser(description='CDR output handler module.')

    # [*]Mandatory parameters.
    parser.add_argument('--id', type=str, help='ID of ML processor', default="main")

    # [*]Hyper parameters.
    parser.add_argument('--sleep', type=int, help='Sleep time.(Default:60)', default=60)
    parser.add_argument('--log', type=str, help='Set log level', default="INFO")

    args = parser.parse_args()

    fp.IDX = args.id

    sleep_time = args.sleep
    LOG_LEVEL = args.log

    # [*]Make Final output directory, if doesn't exist.
    directory_check()

    '''
        Graceful killer
    '''
    killer = Clean()

    # [*]Every day logging in different file.
    today = datetime.now().date()
    tomorrow = today + timedelta(days=1)

    elog_path = fp.log_dir() + 'output_handler_error_{}.log'.format(today)
    log_path = fp.log_dir() + 'output_handler_{}.log'.format(today)

    elogger = FileLogger("output_handler_error", elog_path, level="WARNING").get_instance()
    slogger = StreamLogger("stream_output_handler", level=STREAM_LOG_LEVEL).get_instance()
    logger = FileLogger("output_handler", log_path, level=LOG_LEVEL).get_instance()

    if os.path.exists(fp.run_dir() + "output_handler.run"):
        elogger.error("output handler is already running. Program exit.")
        mk.debug_info("output handler is already running. Program exit.", m_type="ERROR")
    else:
        with open(fp.run_dir() + "output_handler.run", "w") as run_file:
            run_file.write(str(os.getpid()))

    mk.debug_info("output_handler starts running.")
    main()
