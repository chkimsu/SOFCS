"""
@ File name: anomaly_detector.py
@ Version: 1.3.2
@ Last update: 2020.JAN.15
@ Author: DH.KIM
@ Company: Ntels Co., Ltd
"""

import json
import csv
import os
import config.file_path as fp

from models.rrcf_cls import RRCF
from utils.queue import Queue

LOG_LEVEL = "INFO"


class AnomalyDetector(object):
    """
    Compute anomaly score and updates the threshold limit within certain period.
    Also it determines probability of anomaly. This module generates a file as an output.
        1) Compute the anomaly score with input data
        2) Updates the threshold limit
        3) Determine anomaly
        4) Writing a result in file.
    """

    def __init__(self, num_trees, leaves_size, sequences, quantile=0.99, ip='Unknown', svc_type='Unknown'):
        """
        Initialize the rrcf module, maximum threshold duration, and quantile value.
        :param num_trees: An integer. The number of trees.
        :param leaves_size: An integer. The size of leaves.
        :param sequences: An integer. The observing points.
        :param quantile: An float. Quantile value.
        :param ip: A String. IP address of p-gateway.
        :param svc_type: A String. Service type.
        """
        # [*]Create RRCF realtime detection object.
        self.rrcf = RRCF(num_trees, sequences, leaves_size)
        # [*]Update duration of threshold value.
        self.max_threshold_duration = sequences * 24 * 60 * 30  # 30 days sequences = (24 hours * 60 minutes * 30 days)
        # [*]Collecting anomaly scores
        self.anomaly_score = []
        # [*]Anomaly counter queue
        self.aq = AnomayQueue(sequences)
        # [*]Sensitiveness of anomaly score.
        self.quantile = quantile

        # [*]For writing file.
        self.ip = ip
        self.svc_type = svc_type

    def compute_anomaly_score(self, date, data, output_path, dlogger):
        """
        Calculate anomaly score, calculate threshold, and determine anomaly.
        :param date: A numpy array. Date and time of input training data.
        :param data: A numpy array. Input training data.
        :param output_path: A String. The path of output result.
        :return: None.
        """

        # [*]Calculate the anomaly score.
        r = self.rrcf.anomaly_score(date, data, with_date=True)
        self.anomaly_score.append(r)

        # [*]Calculate threshold.
        self._calculate_threshold()

        # [*]Determine anomaly.
        output_result = self._determine_anomaly()

        if output_result['percentage'] == 'observing':
            final_result = [self.ip, date[-1], self.svc_type, data[-1][0], data[-1][1],
                            output_result['score'], output_result['estimate']]
        elif output_result['percentage'] == 'Normal':
            final_result = [self.ip, date[-1], self.svc_type, data[-1][0], data[-1][1],
                            output_result['score'], output_result['estimate']]
        else:
            final_result = [self.ip, date[-1], self.svc_type, data[-1][0], data[-1][1],
                            output_result['score'], output_result['estimate'], output_result['percentage'][-1]]

        # [*]log the result
        dlogger.info(output_result)

        # [*]Write the result in a file.
        with open(output_path, 'w') as file:
            csv_writer = csv.writer(file, delimiter='|')
            csv_writer.writerow(final_result)
            dlogger.debug("{} is written successfully.".format(output_path))

        with open(output_path + ".INFO", 'w') as file:
            file.write("")
            dlogger.debug("{} is written successfully.".format(output_path+".INFO"))

    def _calculate_threshold(self):
        """
        Calculate threshold and update in this object.
        :return: None
        """
        # [*] Make anomaly directory if doesn't exist.
        if not os.path.exists(fp.anomaly_score_dir(self.ip, self.svc_type)):
            os.makedirs(fp.anomaly_score_dir(self.ip, self.svc_type))

        if len(self.anomaly_score) < self.max_threshold_duration:
            # [*]If less than 30 days it will update threshold.
            self.rrcf.threshold = self.rrcf.calc_threshold(self.anomaly_score, self.quantile, with_data=False)

        # [*]After 30 days, re-calculates threshold.
        if len(self.anomaly_score) >= (self.max_threshold_duration * 2):
            start_date = self.anomaly_score[0][0]
            end_date = self.anomaly_score[self.max_threshold_duration][0]

            anomaly_score_path = fp.anomaly_score_dir(self.ip, self.svc_type) \
                                 + "anomaly_score_{}_{}.json".format(start_date, end_date)
            # [*]Save the previous results.
            with open(anomaly_score_path, 'w') as file:
                json.dump(self.anomaly_score[:self.max_threshold_duration], file)
            self.anomaly_score = self.anomaly_score[self.max_threshold_duration:]
            self.rrcf.threshold = self.rrcf.calc_threshold(self.anomaly_score, self.quantile, with_data=False)

    def _determine_anomaly(self):
        """
        Determine anomaly using recorded anomaly queue.
        If observing mode active, they are not judged yet. Collect the data until anomaly queue is full.
        If queue is full, it determines anomaly.
        :return: None
        """
        date = self.anomaly_score[-1][0]
        score = self.anomaly_score[-1][1]
        percentage = 'observing'

        if self.aq.active_mode:
            # [*]If anomaly Queue is activated already.
            if score >= self.rrcf.threshold:
                # [*]When anomaly point detected.
                self.aq.put([date, 'anomaly'])
                result = {
                    'date': date,
                    'score': score,
                    'estimate': 'Anomaly',
                    'percentage': percentage
                }
            else:
                # [*]When normal point detected.
                self.aq.put([date, 'normal'])
                result = {
                    'date': date,
                    'score': score,
                    'estimate': 'Normal',
                    'percentage': percentage
                }

            if self.aq.full():
                # [*]If queue is full, calculate percentage.
                result['percentage'] = self.aq.anomaly_determination(base=self.rrcf.sequences)
                self.aq.clear()
                self.aq.active_mode = False

            return result

        else:
            # [*]If anomaly Queue is non-activated.
            if score >= self.rrcf.threshold:
                # [*]When anomaly point detected.
                self.aq.active_mode = True
                self.aq.put([date, 'anomaly'])
                result = {
                    'date': date,
                    'score': score,
                    'estimate': 'Anomaly',
                    'percentage': percentage
                }
            else:
                # [*]When normal point detected.
                result = {
                    'date': date,
                    'score': score,
                    'estimate': 'Normal',
                    'percentage': 'Normal'
                }
        return result


class AnomayQueue(Queue):
    """
    This Queue class is designed to calculate anomaly probabilities.
    """

    def __init__(self, size):
        super().__init__(size)
        self.active_mode = False

    def clear(self):
        self.indexList = []

    def anomaly_determination(self, base):
        anomaly_counter = 0
        for item in self.indexList:
            if item[1] == 'anomaly':
                anomaly_counter += 1
        p = round(anomaly_counter / base, 3)
        percentage = [self.indexList[0][0], self.indexList[-1][0], p]
        return percentage
