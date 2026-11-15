"""带 LSN 的独立仓页。"""
import json
from .frames import encode, scan

class Pages:
    def __init__(self, device):
        self.device = device

    def read(self, page_id):
        raw = self.device.read('page/' + str(page_id))
        if not raw:
            return {'page_id': page_id, 'lsn': 0, 'value': None}
        frames, consumed, torn = scan(raw)
        if torn or consumed != len(raw) or len(frames) != 1 or frames[0]['page_id'] != page_id:
            raise ValueError('页内容损坏')
        return frames[0]

    def write(self, page_id, lsn, value, flush=False):
        if type(lsn) is not int or lsn <= 0:
            raise ValueError('页LSN必须为正')
        name = 'page/' + str(page_id)
        self.device.write(name, encode({'page_id': page_id, 'lsn': lsn, 'value': value}))
        if flush:
            self.device.sync(name)
