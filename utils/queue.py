"""
@ File name: queue.py
@ Version: 1.1.1
@ Last update: 2020.JAN.15
@ Author: DH.KIM
@ Company: Ntels Co., Ltd
"""
import utils.marker as marker


class Queue(object):
    def __init__(self, size=0):
        """ Customized queue list.

        Args:
            :param size: An integer. Max size of queue.
        """
        self.size = size
        self.indexList = []

    def put(self, index):
        """
        Insert the item
        Args:
            :param index: Any type. It could be any value type.
            :return:
                - None or Error if buffer is full.
        """
        if self.size != 0:
            if len(self.indexList) > self.size:
                marker.debug_info("Buffer overflow. Queue should not exceed the size.", m_type="ERROR")
                raise SystemExit()
        self.indexList.append(index)

    def get(self):
        """
        Get the item. FIFO.
        :return:
            - value: Any type. First item in list.
            - SystemExit(): If Queue is empty
        """
        if len(self.indexList) == 0:
            marker.debug_info("Queue is empty", m_type="ERROR")
            raise SystemExit()
        value = self.indexList[0]
        self.indexList.remove(value)
        return value

    def empty(self):
        """
        Returns True if queue list is empty.
        :return:
            - Boolean
        """
        if len(self.indexList) != 0:
            return False
        else:
            return True

    def full(self):
        """
        Returns True if queue list is full.
        :return:
            - Boolean
            - None: If buffer limit is not defined in advance.
        """
        if self.size == 0:
            marker.debug_info("Queue object has no buffer limit. The \'full\' method is useless in this case.",
                              m_type="WARNING")
            return None
        else:
            if len(self.indexList) >= self.size:
                return True
            else:
                return False

    def queue_length(self):
        """
        Returns queue length
        :return:
            An Integer. Queue length.
        """
        return len(self.indexList)

    def queue_status(self):
        """
        Returns queue status
        :return:
            An String: returns Queue status either Full, Empty or length
        """
        if self.full():
            return 'full'
        elif self.empty():
            return 'empty'
        else:
            return str(self.queue_length())
