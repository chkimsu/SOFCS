"""
@ File name: training_module.py
@ Version: 1.1.1
@ Last update: 2019.Oct.23
@ Author: DH.KIM
@ Company: Ntels Co., Ltd
"""

import pandas as pd
import pickle
import os
import dill
import glob
import config.pgw_ip_address as pgw_ip_list
import utils.marker as marker
import argparse
from models.rrcf_cls import RRCF


def data_separation(pgw_ip, svc_type):
    """
    Data load from csv file.
    """
    df = pd.read_csv("./data/{}/{}.csv".format(pgw_ip, svc_type))
    df['DTmm'] = pd.to_datetime(df['DTmm'], format='%Y-%m-%d %H:%M')
    df_train = df.loc[df['DTmm'] < '2019-08-01']
    df_test = df.loc[df['DTmm'] >= '2019-08-01'].reset_index(drop=True)

    return df_train, df_test


def train_models(data, num_of_trees, sequences, num_of_leaves, quantile, write_file=False):
    date = data['data']['DTmm']
    train_data = data['data'][['Real_Up', 'Real_Dn']]
    train_data = train_data.to_numpy()
    o_rrcf = RRCF(num_trees=num_of_trees, sequences=sequences, leaves_size=num_of_leaves)
    score, ftime = o_rrcf.train_rrcf(date, train_data, timer=True)
    marker.debug_info("Required time: {}".format(ftime))

    _ = o_rrcf.calc_threshold(score, quantile, with_data=False)
    marker.debug_info("Threshold: {}".format(o_rrcf.threshold))

    if write_file:
        instance_path = "./{}/{}/{}/".format(INSTANCE_DIR, data['pgw_ip'], data['svc_type'])

        if not os.path.exists(instance_path):
            marker.debug_info("Instance directory is not exist. Creating one...")
            os.makedirs(instance_path)

        with open(instance_path + "hyper_parameter.txt", "w") as file:
            file.write("number of trees: {}\n".format(o_rrcf.num_trees))
            file.write("number of leaves: {}\n".format(o_rrcf.leaves_size))
            file.write("sequences: {}\n".format(o_rrcf.sequences))
            file.write("required time: {}\n".format(ftime))

        with open(instance_path + "model.pkl", "wb") as output:
            dill.dump(o_rrcf, output)

        with open(instance_path + "anomaly_scores.dict", "wb") as file:
            dill.dump(score, file)


def load(pgw_ip, svc_type):
    with open('./{}/{}/{}/model.pkl'.format(INSTANCE_DIR, pgw_ip, svc_type), "rb") as file:
        rrcf_object = pickle.load(file)
    return rrcf_object


def main(num_of_trees, num_of_leaves, sequences, quantile=0.99):
    l_pgw_ip = pgw_ip_list.l_pgw_ip

    for pgw_ip in l_pgw_ip:

        marker.debug_info("Running \'pgw_ip - {}\'".format(pgw_ip))
        l_svc_type = glob.glob("./data/{}/*.csv".format(pgw_ip))

        for fname in l_svc_type:

            name = fname.split('/')[-1]
            svc_type = name.split('.')[0]

            marker.debug_info("\t \'svc_type - {}\'".format(svc_type))

            df_train, df_test = data_separation(pgw_ip, svc_type)

            if len(df_train.index) < sequences:
                # NOTE: If there are not enough data length, it will pass
                if not os.path.exists('./error_report/'):
                    os.mkdir('./error_report')
                with open("./error_report/untrained_model.txt", "a") as file:
                    file.write("{}::{}\n".format(pgw_ip, fname))
                continue

            data = {
                'pgw_ip': pgw_ip,
                'svc_type': svc_type,
                'data': df_train
            }

            try:
                train_models(data, num_of_trees, sequences, num_of_leaves, quantile, write_file=True)
            except Exception as e:
                marker.debug_info("PGW IP: {} / SVC_TYPE: {} / Error occurs: {}".format(pgw_ip, svc_type, e))
                with open("./error_report/untrained_model.txt", "a") as file:
                    file.write("{}::{} - Error: {}\n".format(pgw_ip, fname, e))
                continue


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='CDR anomaly detector training module.')
    parser.add_argument('--trees', type=int, help='Number of trees.(Default:80)', default=80)
    parser.add_argument('--sequences', type=int, help='Sequences to observe.(Default: 5)', default=5)
    parser.add_argument('--leaves', type=int, help='Leaf size to memorize.(Default: 1440)', default=1440)
    parser.add_argument('--dir_name', type=str, help='Directory name for object', default='instances')

    args = parser.parse_args()

    INSTANCE_DIR = args.dir_name

    main(args.trees, args.leaves, args.sequences)
