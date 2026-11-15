"""可确定注入断电的存储设备。"""
from copy import deepcopy

class PowerLoss(RuntimeError):
    pass

class Device:
    def __init__(self, durable=None, fail_at=None):
        self.durable = deepcopy(durable or {})
        self.pending = deepcopy(self.durable)
        self.trace = []
        self.fail_at = fail_at

    def step(self, action):
        self.trace.append(action)
        if len(self.trace) == self.fail_at:
            raise PowerLoss(action)

    def read(self, name):
        return self.pending.get(name, b'')

    def write(self, name, raw):
        self.step(('before_write', name))
        self.pending[name] = bytes(raw)
        self.step(('after_write', name))

    def append(self, name, raw):
        self.write(name, self.read(name) + raw)

    def sync(self, name):
        self.step(('before_sync', name))
        if name in self.pending:
            self.durable[name] = self.pending[name]
        self.step(('after_sync', name))

    def replace(self, source, target):
        self.step(('before_replace', source, target))
        self.pending[target] = self.pending.pop(source)
        self.durable[target] = self.durable.pop(source)
        self.step(('after_replace', source, target))

    def crash(self):
        return Device(self.durable)
