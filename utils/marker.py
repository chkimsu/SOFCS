"""
@ File name: marker.py
@ Version: 1.0.1
@ Last update: 2019.Oct.23
@ Author: DH.KIM
@ Company: Ntels Co., Ltd
"""
from inspect import getframeinfo, stack


def debug_info(message, m_type='INFO'):
    insp = getframeinfo(stack()[1][0])
    if m_type == "INFO":
        print("[*] INFO:\n"
              "\t - File: {}:{}\n"
              "\t - Message: {}".format(insp.filename, insp.lineno, message))
    elif m_type == "WARNING":
        print("[@] WARNING:\n"
              "\t - File: {}:{}\n"
              "\t - Message: {}".format(insp.filename, insp.lineno, message))
    elif m_type == "ERROR":
        print("[!] ERROR:\n"
              "\t - File: {}:{}\n"
              "\t - Message: {}".format(insp.filename, insp.lineno, message))
        raise SystemExit
    else:
        raise ValueError("Invalid input vlaue \'m_type\': {}".format(m_type))
