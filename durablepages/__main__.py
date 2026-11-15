import json
from .device import Device
from .pages import Pages
device = Device()
Pages(device).write(1, 1, {'venue': '市集', 'rows': 2}, flush=True)
print(json.dumps(Pages(device.crash()).read(1), ensure_ascii=False))
