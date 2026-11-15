import unittest
from durablepages.device import Device, PowerLoss
from durablepages.frames import encode, scan
from durablepages.pages import Pages

class CoreTests(unittest.TestCase):
    def test_unsynced_bytes_vanish(self):
        device = Device()
        Pages(device).write(1, 1, 80)
        self.assertIsNone(Pages(device.crash()).read(1)['value'])
        device.sync('page/1')
        self.assertEqual(Pages(device.crash()).read(1)['value'], 80)

    def test_torn_tail_and_corrupt_complete_frame(self):
        first, second = encode({'lsn': 1}), encode({'lsn': 2})
        self.assertEqual(scan(first + second[:-1]), ([{'lsn': 1}], len(first), True))
        changed = bytearray(first)
        changed[-2] ^= 1
        with self.assertRaisesRegex(ValueError, '损坏'):
            scan(bytes(changed))

    def test_powerloss_after_sync_can_be_durable(self):
        device = Device(fail_at=4)
        with self.assertRaises(PowerLoss):
            device.write('a', b'yes')
            device.sync('a')
        self.assertEqual(device.crash().read('a'), b'yes')

    def test_page_identity(self):
        device = Device()
        device.write('page/2', encode({'page_id': 1, 'lsn': 3, 'value': 7}))
        with self.assertRaises(ValueError):
            Pages(device).read(2)
