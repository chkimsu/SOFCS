"""
@ File name: anomaly_detection.py
@ Version: 1.3.6
@ Last update: 2020.JAN.15
@ Author: DH.KIM
@ Company: Ntels Co., Ltd
"""

import pandas as pd
import os
import config.file_path as file_path
import argparse
import time
import glob
import numpy as np
import traceback
import timeit
import utils.marker as mk
import dill
import pickle

from models.anomaly_detector import AnomalyDetector
from datetime import datetime, timedelta
from utils.queue import Queue
from utils.logger import FileLogger, StreamLogger
from utils.graceful_killer import GracefulKiller

SLOG_LEVEL = "INFO"


class Clean(GracefulKiller):
    def __init__(self, ip, svc):
        super().__init__()
        self.ip = ip
        self.svc = svc

    def exit_gracefully(self, signum, frame):
        # [*]Process killed by command or Keyboard Interrupt.
        os.remove(file_path.run_dir() + "{}_{}.detector.run".format(self.ip, self.svc))
        slogger.info("anomaly detector - {}:{} is end.".format(self.ip, self.svc))
        self.kill_now = True
        logger.debug("Program killed by signal.")
        # raise SystemExit


def data_loader(input_dir):
    """
    Load the train data from input directory.
    :param input_dir: A String. Train data path.
    :return:
        - queue: A Queue object.
        - boolean: if file exist returns True, else returns False.
    """
    stime = timeit.default_timer()

    info_file_list = glob.glob(input_dir + "*.DAT.INFO")
    if info_file_list:
        info_file_list = sorted(info_file_list)

        # [*] Work with first file
        file = info_file_list[0]
        logger.info(".INFO file is detected: {}".format(file))

        # [*]Remove .INFO extension.
        file = file[:-5]
        df = pd.read_csv(file, delimiter='|', names=['PGW_IP', 'DTmm', 'SVC_TYPE', 'UP', 'DN'], dtype={
            "PGW_IP": str,
            "DTmm": str,
            "SVC_TYPE": str,
            "UP": float,
            "DN": float
        }).to_numpy()

        logger.info("Data file is opened: {}".format(file))
        logger.info("Dataframe: {}".format(df))

        # [*]Remove loaded file list.
        os.remove(info_file_list[0])
        os.remove(file)

        logger.debug(".INFO file is removed: {}".format(info_file_list[0]))
        logger.debug(".DAT file is removed: {}".format(file))

        etime = timeit.default_timer()
        logger.info("Data loader required time: {}".format(etime - stime))

        return df
    return None


def detection(detector, data, output_dir):
    """
    Compute the anomaly scores and write a output into a file.
    :param detector: An Anomaly Detector object. Anomaly Detector that contains its ip address and service type.
    :param data: A Queue object. Input training data.
    :param output_dir: A String. Output directory path.
    :return: None
    """

    for d in data:
        if dstore.full():
            dstore.get()
            dstore.put([d[1], d[3:]])
            training_data = np.array(dstore.indexList)
            t_date = training_data[:, 0]
            t_data = training_data[:, 1]

            np_data = []
            for t in t_data:
                np_data.append(np.array(t, dtype=np.float))
            np_data = np.array(np_data)
            logger.info("Detection input data ({})".format(np_data))
            output_path = output_dir + '{}_{}_{}.DAT'.format(detector.ip, detector.svc_type, t_date[-1])
            detector.compute_anomaly_score(t_date, np_data, output_path, detector_logger)
            logger.info("Threshold value: {}".format(detector.rrcf.threshold))
        else:
            dstore.put([d[1], d[3:]])
            logger.debug("dstore: {}".format(dstore.indexList))


def directory_check():
    # [*]Create directory if doesn't exist.
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
        slogger.debug("LOG directory doesn't exist. Create one; ({})".format(LOG_DIR))

    if not os.path.exists(INPUT_DIR):
        os.makedirs(INPUT_DIR)
        slogger.debug("INPUT directory doesn't exist. Create one; ({})".format(INPUT_DIR))

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        slogger.debug("OUTPUT directory doesn't exist. Create one; ({})".format(OUTPUT_DIR))

    if not os.path.exists(FINAL_OUTPUT_DIR):
        os.makedirs(FINAL_OUTPUT_DIR)
        slogger.debug("FINAL_OUTPUT directory doesn't exist. Create one; ({})".format(FINAL_OUTPUT_DIR))

    if not os.path.exists(RUN_DIR):
        os.makedirs(RUN_DIR)
        slogger.debug("RUNNING directory doesn't exist. Create one; ({})".format(RUN_DIR))

    if not os.path.exists(INSTANCE_DIR):
        os.makedirs(INSTANCE_DIR)
        slogger.debug("INSTANCE_DIR directory doesn't exist. Create one; ({})".format(RUN_DIR))


def main(ip, svc, t, l, seq, q):
    """
    Work flow:
        1) Directory creation, if doesn't exist.
        2) Logger define.
        3) Anomaly Detector define.
        4) While roof
            4-1) Check logger's date.
            4-2) Loading the data, if input file exists.
            4-3) Anomaly detection, if data queue is full and file is read.

    :param ip: A String. P-gateway address.
    :param svc: A String. Service Type.
    :param t: An Integer. Number of trees.
    :param l: An Integer. Leaf size.
    :param seq: An Integer. Sequences.
    :param q: A Float. Quantile.
    :return: None.
    """
    global slogger, logger, elogger, detector_logger, elog_path
    global today, tomorrow
    global dstore
    global LOG_LEVEL
    global anomaly_detector

    logger.info("\n\t\t@Hyper parameters: \n"
                "\t\t\t+IP addr: {}\n"
                "\t\t\t+SVC type: {}\n"
                "\t\t\t+Trees: {}\n"
                "\t\t\t+Leaves: {}\n"
                "\t\t\t+Sequences: {}\n"
                "\t\t\t+Quantile: {}".format(ip, svc, t, l, seq, q))

    '''
        Initialize Graceful Killer
    '''
    killer = Clean(ip, svc)

    try:
        if os.path.exists(INSTANCE_DIR + "model.pkl"):
            with open(INSTANCE_DIR+"model.pkl", "rb") as model:
                anomaly_detector = pickle.load(model)
            slogger.info("Model is already exist. Loaded successfully!")
            logger.info("Anomaly Detector successfully loaded.")
            logger.info(anomaly_detector.rrcf.forest)
        else:
            anomaly_detector = AnomalyDetector(t, l, sequences=seq, quantile=q, ip=ip, svc_type=svc)
            logger.info("Anomaly Detector successfully created.")

        if os.path.exists(INSTANCE_DIR + "dstore.pkl"):
            with open(INSTANCE_DIR + "dstore.pkl", "rb") as ds:
                dstore = pickle.load(ds)

    except Exception:
        elogger.error(traceback.format_exc())
        slogger.error("Anomaly Detector couldn't be created. Check your error log: {}".format(elog_path))
        os.remove(file_path.run_dir() + "{}_{}.detector.run".format(ip, svc))
        model_save()
        raise SystemExit

    while not killer.kill_now:
        # [*] Check directory existence.
        directory_check()

        # [*]If Day pass by create a new log file.
        today = datetime.now().date()
        if today >= tomorrow:
            tomorrow = today + timedelta(days=1)

            # [*]Log handler updates
            update_log_path = file_path.svc_log_dir(ip, svc) + 'anomaly_detection_{}.log'.format(today)
            update_elog_path = file_path.svc_log_dir(ip, svc) + 'anomaly_detection_error_{}.log'.format(today)
            update_dlog_path = file_path.svc_log_dir(ip, svc) + 'anomaly_detector_{}.log'.format(today)

            slogger = StreamLogger('anomaly_detection_stream_logger', level=SLOG_LEVEL).get_instance()
            logger = FileLogger('anomaly_detection_info', log_path=update_log_path, level=LOG_LEVEL).get_instance()
            elogger = FileLogger('anomaly_detection_error', log_path=update_elog_path, level='WARNING').get_instance()
            detector_logger = FileLogger('anomaly_detector', log_path=update_dlog_path, level=LOG_LEVEL).get_instance()

        try:
            # [*]Loading the data and save it into queue.
            data = data_loader(INPUT_DIR)
            slogger.debug("Read status: {}".format(data))
        except Exception:
            elogger.error(traceback.format_exc())
            slogger.error("Data loader can't work properly. Check your error log: {}".format(elog_path))
            os.remove(file_path.run_dir() + "{}_{}.detector.run".format(ip, svc))
            model_save()
            raise SystemExit

        try:
            if data is not None:
                stime = timeit.default_timer()
                # [*]Anomaly Detection.
                detection(anomaly_detector, data, OUTPUT_DIR)
                etime = timeit.default_timer()
                logger.info("Detection required time: {}".format(etime - stime))
                slogger.debug("Detection is normally worked.")
            time.sleep(1)
        except Exception:
            elogger.error(traceback.format_exc())
            slogger.error("Detection method didn't work properly. Check your error log: {}".format(elog_path))
            os.remove(file_path.run_dir() + "{}_{}.detector.run".format(ip, svc))
            model_save()
            raise SystemExit

    model_save()


def model_save():
    with open(INSTANCE_DIR + "model.pkl", "wb") as output:
        dill.dump(anomaly_detector, output)
        logger.info("Model is saved..")

    with open(INSTANCE_DIR + "dstore.pkl", "wb") as output:
        dill.dump(dstore, output)
        logger.info("Data queue is saved : {}".format(dstore))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='CDR anomaly detection module.')
    # [*]Mandatory parameters.
    parser.add_argument('--ip', type=str, help='IP address of services.', required=True)
    parser.add_argument('--svc', type=str, help='Service Type', required=True)
    parser.add_argument('--id', type=str, help='ID of ML processor', default="main")

    # [*]Hyper parameters.
    parser.add_argument('--trees', type=int, help='Number of trees.(Default:80)', default=80)
    parser.add_argument('--seq', type=int, help='Sequences to observe.(Default: 6)', default=6)
    parser.add_argument('--leaves', type=int, help='Leaf size to memorize.(Default: 864)', default=864)
    parser.add_argument('--q', type=float, help='Quantile value.(Default: 0.99)', default=0.99)
    parser.add_argument('--log', type=str, help='Set log level', default="INFO")

    args = parser.parse_args()

    file_path.IDX = args.id

    LOG_DIR = file_path.svc_log_dir(args.ip, args.svc)
    INPUT_DIR = file_path.input_dir(args.ip, args.svc)
    OUTPUT_DIR = file_path.output_dir(args.ip, args.svc)
    INSTANCE_DIR = file_path.instant_dir(args.ip, args.svc)
    FINAL_OUTPUT_DIR = file_path.final_output_path()
    RUN_DIR = file_path.run_dir()
    LOG_LEVEL = args.log

    '''
        - slogger: Stream logger.
    '''
    slogger = StreamLogger('anomaly_detection_stream_logger', level=SLOG_LEVEL).get_instance()

    # [*] Check directory existence.
    directory_check()

    # [*]Every day logging in different fie.
    today = datetime.now().date()
    tomorrow = today + timedelta(days=1)

    log_path = file_path.svc_log_dir(args.ip, args.svc) + 'anomaly_detection_{}.log'.format(today)
    elog_path = file_path.svc_log_dir(args.ip, args.svc) + 'anomaly_detection_error_{}.log'.format(today)
    dlog_path = file_path.svc_log_dir(args.ip, args.svc) + 'anomaly_detector_{}.log'.format(today)

    '''
        - logger; Informative logger.
        - elogger; error logger.
    '''
    logger = FileLogger('anomaly_detection_info', log_path=log_path, level=LOG_LEVEL).get_instance()
    elogger = FileLogger('anomaly_detection_error', log_path=elog_path, level='WARNING').get_instance()
    detector_logger = FileLogger('anomaly_detector', log_path=dlog_path, level=LOG_LEVEL).get_instance()

    if os.path.exists(RUN_DIR + '{}_{}.detector.run'.format(args.ip, args.svc)):
        elogger.error("Anomaly detector of {}:{} is already running. Program exit.".format(args.ip, args.svc))
        slogger.error("Anomaly detector of {}:{} is already running. Program exit.".format(args.ip, args.svc))
        raise SystemExit
    else:
        with open(RUN_DIR + '{}_{}.detector.run'.format(args.ip, args.svc), "w") as out:
            out.write(str(os.getpid()))

    mk.debug_info("Anomaly detector({}, {}) start running.".format(args.ip, args.svc))

    # [*] NOTE: Global Queue
    dstore = Queue(args.seq)

    main(args.ip, args.svc, args.trees, args.leaves, args.seq, args.q)
