"""
@ File name: rrcf_cls.py
@ Version: 1.4.2
@ Last update: 2019.Nov.01
@ Author: DH.KIM, YH.HU
@ Company: Ntels Co., Ltd
"""
import models.rrcf as rrcf
import models.shingle as shingle
import timeit
import pandas as pd
import utils.marker as marker
from utils.queue import Queue


class RRCF(object):
    def __init__(self, num_trees, sequences, leaves_size):
        """Create RRCF object that contains train and emit anomaly scores.

        Args:
            :param num_trees: An integer. The number represents number of trees to train.
            :param sequences: An integer. The input sequences of data. Note that if the sequences size is too small,
                then the random cut forest is more sensitive to small fluctuations in the data,
                However, if the shingle size is too large, then smaller scale anomalies might be lost.
            :param leaves_size: An integer. This parameter dictates how many randomly sampled training data points are sent
                to each tree.
        """
        self.num_trees = num_trees
        self.sequences = sequences
        self.leaves_size = leaves_size
        self.index_queue = Queue(size=self.leaves_size)
        self.forest = None
        self.threshold = None

    def train_rrcf(self, date_time, data, timer=False):
        """
        Training the RRCF(Robust Random Cut Forest) model using given data.
        Args:
            :param date_time: A Datatime object. Date and time for data recorded.
            :param data: A Numpy object. The n-dimension data for input.
            :param timer: A Boolean. Returns training time.
            :return:
                - avg_codisp: A dictionary. The Collusive displacement(anomaly score)
                - training time
        """
        if self.forest is not None:
            flag = input("[@] Warning:\n"
                         "\tForest is already exist. Do you want to override? y/[n]: ") or 'n'
            if flag.lower() != 'y':
                return None

        # NOTE: Timer for function execution time.
        train_start = timeit.default_timer()

        self.forest = []
        # NOTE: Build a forest.
        for _ in range(self.num_trees):
            tree = rrcf.RCTree()
            self.forest.append(tree)

        # NOTE: Build a sequences points.
        points = shingle.shingle(data, size=self.sequences)

        # NOTE: Initialize the average of Collusive Displacement(CoDisp).
        avg_codisp = {}
        remove_index = None

        for index, point in enumerate(points):
            # NOTE: For each tree in the forest...
            if self.index_queue.full():
                # NOTE: If leaves are full, get first index in queue(FIFO).
                remove_index = self.index_queue.get()

            for tree in self.forest:
                # NOTE: If tree is above permitted size, drop the oldest point (FIFO)
                if len(tree.leaves) >= self.leaves_size:
                    tree.forget_point(remove_index)
                # NOTE: Insert the new point into the tree
                tree.insert_point(point, index=index)

                # NOTE: Compute CoDisp on the new point and take the average among all trees
                if not date_time[index+self.sequences-1] in avg_codisp:
                    avg_codisp[date_time[index+self.sequences-1]] = 0
                avg_codisp[date_time[index+self.sequences-1]] += tree.codisp(index) / self.num_trees

            # NOTE: Insert new points
            self.index_queue.put(index)

        # NOTE: Timer for function execution time.
        train_end = timeit.default_timer()

        if timer:
            return avg_codisp, train_end-train_start
        else:
            return avg_codisp

    def anomaly_score(self, date, data, with_date=False):
        """
        Compute anomaly score using trained model.
        :param date: A List object. Date and time for request anomaly score.
        :param data: A Numpy array. The n-dimension data to get anomaly score.
        :param with_date: A Boolean. Returns date if it is True.
        :return:
            - avg_codisp: A Float. The Collusive displacement(anomaly score).
            - date: A Numpy array. The last date of anomaly score occurs.
        """
        if self.forest is None:
            marker.debug_info("There is no pre-trained model. It will train the new model.", m_type="INFO")

        avg_codisp = 0
        insert_index = -1

        # NOTE: Get index
        if self.index_queue.full():
            # NOTE: If queue is full, remove first index.
            index = self.index_queue.get()
        elif self.index_queue.empty():
            # NOTE: If queue is empty, initialize the index.
            index = 0
            self.forest = []
            # NOTE: Build a forest.
            for _ in range(self.num_trees):
                tree = rrcf.RCTree()
                self.forest.append(tree)
        else:
            # NOTE: Get last number of index queue.
            index = self.index_queue.indexList[-1]
            index += 1

        # NOTE: Adding a node to the tree
        for tree in self.forest:
            if len(tree.leaves) >= self.leaves_size:
                tree.forget_point(index)

            insert_index = index % self.leaves_size
            tree.insert_point(data, index=insert_index)

            avg_codisp += tree.codisp(insert_index) / self.num_trees

        if insert_index <= -1:
            marker.debug_info("Invalid \'insert_index\' value. We have \'{}\'".format(-1), m_type="ERROR")
            raise SystemExit()

        # NOTE: Inserting new index number
        self.index_queue.put(insert_index)

        if with_date is True:
            return [date[-1], avg_codisp]
        else:
            return avg_codisp

    def calc_threshold(self, score, q, with_data=False):
        """
        Computing the threshold according to given quantile.
        :param score: A Dictionary. The collected anomaly scores.
        :param q: A float. Quantile value (0 < q < 1)
        :param with_data: A Boolean. Returns the original data with threshold.
        :return:
            - threshold
            - anomaly_result
        """

        if q < 0 or q > 1:
            marker.debug_info("Quantile value \'q\' should be range in 0 < q < 1", m_type="ERROR")
            raise SystemExit

        sdf = None

        if type(score) == dict:
            sdf = pd.DataFrame(score.items(), columns=["DATE", "Anomaly_score"])
        elif type(score) == list:
            sdf = pd.DataFrame(score, columns=["DATE", "Anomaly_score"])
        else:
            marker.debug_info('Invalid data type \'{}\''.format(type(score)), m_type='ERROR')

        threshold = sdf.quantile(q=q)

        if with_data:
            anomaly_result = sdf[sdf['Anomaly_score'] >= threshold['Anomaly_score']]
            return threshold['Anomaly_score'], anomaly_result
        else:
            return threshold['Anomaly_score']
