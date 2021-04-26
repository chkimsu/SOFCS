"""
@ File name: graceful_killer.py
@ Version: 1.0.0
@ Last update: 2019.Nov.19
@ Author: DH.KIM
@ Company: Ntels Co., Ltd

@ Referred by https://stackoverflow.com/questions/18499497/how-to-process-sigterm-signal-gracefully
"""
import signal


class GracefulKiller:
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self.kill_now = True
