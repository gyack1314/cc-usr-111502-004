"""事务、检查点与恢复测试。

核心手段是在公开存储替身 Device 的 write/sync/replace 边界逐点注入
PowerLoss，再用掉电后的 durable 像重建存储，核对可见页与回执。
"""
import json
import unittest
from copy import deepcopy

from durablepages import Device, PowerLoss, Store, encode, scan
from durablepages.store import (
    ConflictingRequest, CorruptStorage, WAL_NAME, RECEIPTS_NAME,
    WAL_TMP_NAME,
)


def run_commit(device, rid, writes, flush_pages):
    store = Store.open(device)
    if flush_pages:
        store.stage(rid, writes, flush_pages=True)
        store.commit(rid)
    else:
        store.commit(rid, writes, flush_pages=False)
    return store


def assert_pre_state(store, rid, writes, seed_pages=()):
    assert store.receipt(rid) is None
    for pid in writes:
        assert store.read(int(pid))['value'] is None
    for pid, value in seed_pages:
        assert store.read(pid)['value'] == value


def assert_post_state(store, rid, writes, seed_pages=()):
    receipt = store.receipt(rid)
    assert receipt is not None and receipt['status'] == 'committed'
    for pid, value in writes.items():
        page = store.read(int(pid))
        assert page['value'] == value
        assert page['lsn'] > 0
    for pid, value in seed_pages:
        assert store.read(pid)['value'] == value

def logical_snapshot(store):
    """与具体帧字节无关的逻辑状态，用于崩溃前后等价性比较。"""
    pages = {}
    for pid in sorted(store._page_ids):
        page = store.read(pid)
        if page['lsn'] > 0:  # 墓碑（lsn=0）语义即“页不存在”
            pages[pid] = (page['lsn'], page['value'])
    return {
        'receipts': {
            rid: (r['status'], r['lsn'], r['digest'])
            for rid, r in sorted(store._receipts.items())
        },
        'pages': pages,
        # abort 帧可能因恢复中断重跑而追加，只比较各类记录的存在性。
        'wal_kinds': sorted(
            (r['kind'], r.get('txn'))
            for r in scan(store.device.read(WAL_NAME))[0]
            if r['kind'] != 'abort'),
        'aborted': sorted(
            r.get('txn')
            for r in scan(store.device.read(WAL_NAME))[0]
            if r['kind'] == 'abort'),
    }


def primed_device():
    device = Device()
    Store.open(device).commit('seed', {'3': {'base': 1}})
    return device


class TransactionTests(unittest.TestCase):
    def test_acknowledgement_boundary_and_reopen(self):
        device = Device()
        store = Store.open(device)
        receipt = store.commit('market-load-01', {'1': {'rows': 2},
                                                   '2': {'sum': 120}})
        self.assertEqual(receipt['status'], 'committed')
        self.assertGreater(receipt['lsn'], 0)
        # 已确认事务在全新进程（仅由 durable 重建）后仍然有效。
        reopened = Store.open(Device(deepcopy(device.durable)))
        self.assertEqual(reopened.read(1)['value'], {'rows': 2})
        self.assertEqual(reopened.read(2)['value'], {'sum': 120})
        self.assertEqual(reopened.receipt('market-load-01')['digest'],
                         receipt['digest'])
        # LSN 单调推进，重开后继续增长而不回退。
        lsn_after_reopen = reopened.next_lsn
        second = reopened.commit('market-load-02', {'1': {'rows': 3}})
        self.assertGreater(second['lsn'], receipt['lsn'])
        self.assertGreater(reopened.next_lsn, lsn_after_reopen)

    def test_uncommitted_visible_in_process_but_gone_after_crash(self):
        device = Device()
        store = Store.open(device)
        store.stage('tx', {'1': {'rows': 5}})  # 未 flush，仅 pending
        self.assertEqual(store.read(1)['value'], {'rows': 5})
        reopened = Store.open(device.crash())
        self.assertIsNone(reopened.read(1)['value'])
        self.assertIsNone(reopened.receipt('tx'))

    def test_stolen_pages_rolled_back_after_crash(self):
        # 脏页在提交前就 sync 落盘，不能靠“页永不提前落盘”绕过恢复。
        probe = Device()
        probe_store = Store.open(probe)
        probe_store.stage('tx', {'1': {'rows': 9}}, flush_pages=True)
        probe_store.commit('tx')
        total_steps = len(probe.trace)
        for fail_at in range(1, total_steps + 1):
            with self.subTest(fail_at=fail_at):
                victim = Device()
                victim_store = Store.open(victim)
                victim_store.stage('tx', {'1': {'rows': 9}},
                                   flush_pages=True)
                base = len(victim.trace)
                victim.fail_at = base + fail_at  # 仅在提交阶段计数
                try:
                    victim_store.commit('tx')
                except PowerLoss:
                    pass
                recovered = Store.open(victim.crash())
                # 只有两种合法结局：提交点之前掉电则整体撤销，
                # 提交点之后掉电则完整保留，不存在半组更新。
                if recovered.receipt('tx') is None:
                    self.assertIsNone(recovered.read(1)['value'])
                    self.assertEqual(recovered.read(1)['lsn'], 0)
                else:
                    self.assertEqual(recovered.read(1)['value'],
                                     {'rows': 9})
                    self.assertGreater(recovered.read(1)['lsn'], 0)

    def test_stolen_page_is_genuinely_durable_before_commit(self):
        # 明确验证偷页确实发生：提交前 durable 像里已有新版本页，
        # 恢复机制必须能处理这种状态而不是假装它不会出现。
        device = Device()
        store = Store.open(device)
        store.stage('tx', {'1': {'rows': 9}}, flush_pages=True)
        raw = device.durable['page/1']
        frame = scan(raw)[0][0]
        self.assertEqual(frame['value'], {'rows': 9})
        self.assertEqual(frame['lsn'], 2)
        # 恢复后该未提交页被撤销为不存在语义。
        recovered = Store.open(Device(deepcopy(device.durable)))
        self.assertIsNone(recovered.read(1)['value'])

    def test_commit_sweep_all_or_nothing(self):
        writes = {'1': {'rows': 2}, '2': {'sum': 120}}
        for flush_pages in (False, True):
            with self.subTest(flush_pages=flush_pages):
                probe = Device()
                run_commit(probe, 'tx', writes, flush_pages)
                steps = len(probe.trace)
                for fail_at in range(1, steps + 1):
                    with self.subTest(fail_at=fail_at):
                        device = Device(fail_at=fail_at)
                        with self.assertRaises(PowerLoss):
                            run_commit(device, 'tx', writes, flush_pages)
                        recovered = Store.open(device.crash())
                        if recovered.receipt('tx') is None:
                            assert_pre_state(recovered, 'tx', writes)
                        else:
                            assert_post_state(recovered, 'tx', writes)
                        # 再次重开必须得到完全一致的结论（恢复可重入）。
                        again = Store.open(
                            Device(deepcopy(recovered.device.durable)))
                        self.assertEqual(
                            again.receipt('tx') is None,
                            recovered.receipt('tx') is None)

    def test_recovery_itself_can_be_interrupted(self):
        writes = {'1': {'rows': 7}}
        crashed = Device()
        store = Store.open(crashed)
        store.stage('tx', writes, flush_pages=True)
        crashed.fail_at = len(crashed.trace) + 1
        with self.assertRaises(PowerLoss):
            store.commit('tx')
        image = crashed.crash().durable
        # 无干扰恢复的期望逻辑状态。
        expected = logical_snapshot(Store.open(Device(deepcopy(image))))

        probe = Device(deepcopy(image))
        Store.open(probe)
        for fail_at in range(1, len(probe.trace) + 1):
            with self.subTest(fail_at=fail_at):
                victim = Device(deepcopy(image), fail_at=fail_at)
                with self.assertRaises(PowerLoss):
                    Store.open(victim)
                fixed = Store.open(victim.crash())
                self.assertEqual(logical_snapshot(fixed), expected)
                # 再恢复一次仍然收敛到同一状态。
                fixed_twice = Store.open(
                    Device(deepcopy(fixed.device.durable)))
                self.assertEqual(logical_snapshot(fixed_twice), expected)

    def test_receipt_rebuilt_when_commit_durable_but_receipt_not(self):
        device = Device()
        store = Store.open(device)
        store.stage('tx', {'1': {'rows': 4}}, flush_pages=True)
        # 在 WAL sync 之后、receipts 写入之前制造掉电。
        commit_lsn = store._alloc_lsn()
        store._append({'kind': 'commit', 'lsn': commit_lsn,
                       'txn': 'tx', 'writes': {'1': {'rows': 4}}})
        store._staged.pop('tx')
        device.sync(WAL_NAME)
        self.assertEqual(device.read(RECEIPTS_NAME), b'')
        recovered = Store.open(device.crash())
        receipt = recovered.receipt('tx')
        self.assertIsNotNone(receipt)
        self.assertEqual(recovered.read(1)['value'], {'rows': 4})
        # 回执已被补偿并持久。
        self.assertTrue(Store.open(
            Device(deepcopy(recovered.device.durable))).receipt('tx'))

    def test_duplicate_request_idempotent_conflicting_rejected(self):
        device = Device()
        store = Store.open(device)
        first = store.commit('dup', {'1': {'rows': 2}})
        same = store.commit('dup', {'1': {'rows': 2}})
        self.assertEqual(same, first)
        with self.assertRaises(ConflictingRequest):
            store.commit('dup', {'1': {'rows': 3}})
        with self.assertRaises(ConflictingRequest):
            store.commit('dup', {'2': {'rows': 2}})
        # 冲突拒绝不产生任何日志/页写入。
        trace_len = len(store.device.trace)
        with self.assertRaises(ConflictingRequest):
            store.commit('dup', {'1': {'rows': 3}})
        self.assertEqual(len(store.device.trace), trace_len)

    def test_identity_survives_checkpoint_and_reopen(self):
        device = Device()
        store = Store.open(device)
        receipt = store.commit('keep', {'1': {'rows': 2}})
        store.checkpoint()
        wal_records = scan(store.device.read(WAL_NAME))[0]
        self.assertFalse(
            any(r.get('txn') == 'keep' and r['kind'] == 'commit'
                for r in wal_records))  # commit 已被截断
        reopened = Store.open(Device(deepcopy(store.device.durable)))
        self.assertEqual(reopened.receipt('keep')['digest'],
                         receipt['digest'])
        self.assertEqual(
            reopened.commit('keep', {'1': {'rows': 2}})['digest'],
            receipt['digest'])
        with self.assertRaises(ConflictingRequest):
            reopened.commit('keep', {'1': {'rows': 99}})

    def test_checkpoint_sweep_with_and_without_active_txn(self):
        writes_b = {'2': {'sum': 10}}
        for active in (False, True):
            with self.subTest(active=active):
                probe = primed_device()
                probe_store = Store.open(probe)
                probe_store.commit('b', writes_b)
                if active:
                    probe_store.stage('long', {'1': {'rows': 5}},
                                      flush_pages=True)
                base = len(probe.trace)
                probe_store.checkpoint()
                steps = len(probe.trace) - base
                for delta in range(1, steps + 1):
                    with self.subTest(delta=delta):
                        device = primed_device()
                        store = Store.open(device)
                        store.commit('b', writes_b)
                        if active:
                            # 活动事务在已提交事务之后开启，写不相交页集。
                            store.stage('long', {'1': {'rows': 5}},
                                        flush_pages=True)
                        checkpoint_base = len(device.trace)
                        device.fail_at = checkpoint_base + delta
                        try:
                            store.checkpoint()
                        except PowerLoss:
                            pass
                        recovered = Store.open(device.crash())
                        # 已确认事务不受半截检查点影响。
                        assert_post_state(
                            recovered, 'b', writes_b,
                            seed_pages=((3, {'base': 1}),))
                        self.assertIsNotNone(recovered.receipt('seed'))
                        if active:
                            self.assertIsNone(recovered.receipt('long'))
                            self.assertIsNone(recovered.read(1)['value'])
                        # 未发布的临时日志在恢复后被清掉，绝不参与读取。
                        self.assertEqual(
                            recovered.device.read(WAL_TMP_NAME), b'')
                        # 再开一次结论不变。
                        again = Store.open(
                            Device(deepcopy(recovered.device.durable)))
                        self.assertEqual(
                            logical_snapshot(again),
                            logical_snapshot(recovered))

    def test_active_txn_records_survive_checkpoint_then_commit(self):
        device = Device()
        store = Store.open(device)
        store.stage('long', {'1': {'rows': 5}}, flush_pages=True)
        store.checkpoint()  # 活动事务的 begin/update 必须保留
        wal = scan(store.device.read(WAL_NAME))[0]
        self.assertIn('update', [r['kind'] for r in wal])
        receipt = store.commit('long')
        reopened = Store.open(Device(deepcopy(store.device.durable)))
        self.assertEqual(reopened.receipt('long')['digest'],
                         receipt['digest'])
        self.assertEqual(reopened.read(1)['value'], {'rows': 5})

    def test_abort_restores_before_image(self):
        device = Device()
        store = Store.open(device)
        store.commit('first', {'1': {'rows': 1}})
        store.stage('second', {'1': {'rows': 2}, '2': {'sum': 9}},
                    flush_pages=True)
        store.abort('second')
        self.assertEqual(store.read(1)['value'], {'rows': 1})
        self.assertIsNone(store.read(2)['value'])
        reopened = Store.open(Device(deepcopy(device.durable)))
        self.assertEqual(reopened.read(1)['value'], {'rows': 1})
        self.assertIsNone(reopened.receipt('second'))

    def test_torn_tail_truncated_but_middle_corruption_fatal(self):
        device = Device()
        store = Store.open(device)
        store.commit('tx', {'1': {'rows': 2}})
        wal = device.read(WAL_NAME)
        # 末尾追加半帧：识别为撕裂尾部，截断后正常恢复。
        torn = Device(deepcopy(device.durable))
        torn.write(WAL_NAME, wal + encode({'kind': 'begin', 'lsn': 99,
                                           'txn': 'half'})[:7])
        torn.sync(WAL_NAME)
        recovered = Store.open(torn.crash())
        self.assertEqual(recovered.read(1)['value'], {'rows': 2})
        self.assertEqual(recovered.device.read(WAL_NAME), wal)
        # 中间完整帧被改字节：属于数据损坏，拒绝恢复且不得清空既有页。
        corrupt = Device(deepcopy(device.durable))
        damaged = bytearray(wal)
        damaged[12] ^= 0xFF
        corrupt.write(WAL_NAME, bytes(damaged))
        corrupt.sync(WAL_NAME)
        with self.assertRaises(CorruptStorage):
            Store.open(corrupt.crash())
        self.assertEqual(corrupt.durable.get('page/1'),
                         device.durable.get('page/1'))

    def test_corrupt_page_is_corrupt_storage_not_silently_reset(self):
        device = Device()
        store = Store.open(device)
        store.commit('tx', {'1': {'rows': 2}})
        device.write('page/1', b'garbage-not-a-frame')
        device.sync('page/1')
        with self.assertRaises(CorruptStorage):
            Store.open(device.crash())

    def test_durable_image_roundtrip_as_hex_json(self):
        # 契约：设备持久像可存为十六进制 JSON 交给新进程，恢复不依赖对象。
        device = Device()
        store = Store.open(device)
        store.commit('tx', {'1': {'rows': 2}, '2': {'sum': 120}})
        store.checkpoint()
        blob = json.dumps({name: raw.hex()
                           for name, raw in device.durable.items()})
        loaded = {name: bytes.fromhex(hex_raw)
                  for name, hex_raw in json.loads(blob).items()}
        reopened = Store.open(Device(loaded))
        self.assertEqual(reopened.read(1)['value'], {'rows': 2})
        self.assertEqual(reopened.read(2)['value'], {'sum': 120})
        self.assertIsNotNone(reopened.receipt('tx'))

    def test_redo_after_partial_page_durability(self):
        # commit 已持久但只有部分页 sync：恢复靠 WAL 重放补齐剩余页。
        device = Device()
        store = Store.open(device)
        store.stage('tx', {'1': {'rows': 1}, '2': {'sum': 2}})
        store.device.sync(WAL_NAME)
        # 只手工 sync 第一页，第二页停在 pending。
        store.device.sync('page/1')
        store._append({'kind': 'commit', 'lsn': store._alloc_lsn(),
                       'txn': 'tx', 'writes':
                       {'1': {'rows': 1}, '2': {'sum': 2}}})
        store._staged.pop('tx')
        device.sync(WAL_NAME)
        recovered = Store.open(device.crash())
        self.assertEqual(recovered.read(1)['value'], {'rows': 1})
        self.assertEqual(recovered.read(2)['value'], {'sum': 2})
        self.assertIsNotNone(recovered.receipt('tx'))

    def test_checkpoint_keeps_lsn_monotonic(self):
        device = Device()
        store = Store.open(device)
        first = store.commit('a', {'1': {'rows': 1}})
        store.checkpoint()
        second = store.commit('b', {'1': {'rows': 2}})
        self.assertGreater(second['lsn'], first['lsn'])
        reopened = Store.open(Device(deepcopy(device.durable)))
        # 恢复后页内容与 LSN 不回退，新事务继续单调分配。
        self.assertEqual(reopened.read(1)['value'], {'rows': 2})
        self.assertEqual(reopened.read(1)['lsn'], store.read(1)['lsn'])
        third = reopened.commit('c', {'1': {'rows': 3}})
        self.assertGreater(third['lsn'], second['lsn'])

    def test_serial_commits_on_same_page_chain_before_images(self):
        # 两个已提交事务串行改同一页，崩溃恢复后只保留最终版本。
        device = Device()
        store = Store.open(device)
        store.commit('t1', {'1': {'v': 1}})
        store.commit('t2', {'1': {'v': 2}})
        store.commit('t3', {'1': {'v': 3}})
        reopened = Store.open(Device(deepcopy(device.durable)))
        self.assertEqual(reopened.read(1)['value'], {'v': 3})
        self.assertEqual(
            [reopened.receipt(t)['status'] for t in ('t1', 't2', 't3')],
            ['committed'] * 3)

    def test_crash_during_stage_then_resubmit_after_recovery(self):
        device = Device()
        store = Store.open(device)
        store.stage('tx', {'1': {'v': 1}}, flush_pages=True)
        # 进程掉电，活动事务在新进程里不存在，同身份请求应能重新提交。
        recovered = Store.open(device.crash())
        self.assertIsNone(recovered.receipt('tx'))
        receipt = recovered.commit('tx', {'1': {'v': 1}})
        self.assertEqual(receipt['status'], 'committed')
        again = Store.open(Device(deepcopy(recovered.device.durable)))
        self.assertEqual(again.read(1)['value'], {'v': 1})

    def test_receipts_torn_tail_vs_corruption(self):
        device = Device()
        store = Store.open(device)
        store.commit('tx', {'1': {'v': 1}})
        good = device.durable[RECEIPTS_NAME]
        # 回执账尾部撕裂：截断后由 WAL commit 重新补偿。
        torn = Device(deepcopy(device.durable))
        torn.write(RECEIPTS_NAME,
                   good + encode({'request_id': 'x', 'lsn': 9,
                                  'writes': {}})[:5])
        torn.sync(RECEIPTS_NAME)
        recovered = Store.open(torn.crash())
        self.assertIsNotNone(recovered.receipt('tx'))
        # 回执账完整帧损坏：按 CorruptStorage 上报。
        corrupt = Device(deepcopy(device.durable))
        damaged = bytearray(good)
        damaged[10] ^= 0x01
        corrupt.write(RECEIPTS_NAME, bytes(damaged))
        corrupt.sync(RECEIPTS_NAME)
        with self.assertRaises(CorruptStorage):
            Store.open(corrupt.crash())

    def test_redo_after_checkpoint_truncation_and_crash(self):
        # 检查点后新事务只落了 WAL、页未及刷盘就掉电：
        # 截断后的短 WAL 仍含其 update，redo 补齐。
        device = Device()
        store = Store.open(device)
        store.commit('old', {'1': {'v': 0}})
        store.checkpoint()
        store.stage('new', {'2': {'v': 1}})
        store.device.sync(WAL_NAME)  # 仅 WAL 持久，页停在 pending
        store._append({'kind': 'commit', 'lsn': store._alloc_lsn(),
                       'txn': 'new', 'writes': {'2': {'v': 1}}})
        store._staged.pop('new')
        device.sync(WAL_NAME)
        recovered = Store.open(device.crash())
        self.assertEqual(recovered.read(1)['value'], {'v': 0})
        self.assertEqual(recovered.read(2)['value'], {'v': 1})
        self.assertIsNotNone(recovered.receipt('new'))

    def test_abort_of_txn_that_created_pages_leaves_no_trace_on_reopen(self):
        device = Device()
        store = Store.open(device)
        store.stage('tx', {'1': {'v': 1}, '2': {'v': 2}}, flush_pages=True)
        store.abort('tx')
        reopened = Store.open(Device(deepcopy(device.durable)))
        # 墓碑页（lsn=0）不进入逻辑页号集合（在读页之前检查）。
        self.assertEqual(reopened._page_ids, set())
        self.assertEqual(reopened.read(1)['value'], None)
        self.assertEqual(reopened.read(2)['value'], None)

    def test_full_sequence_failpoint_sweep_invariants(self):
        # 长序列（提交→检查点→偷页提交→检查点→再提交）上对每个设备
        # 边界注入掉电，校验原子性、已确认不丢与恢复可重入。
        page_chain = {
            1: [('t1', {'v': 1}), ('t3', {'v': 3})],
            2: [('t1', {'v': 1}), ('t2', {'v': 2})],
            3: [('t2', {'v': 2})],
        }

        def build(device):
            store = Store.open(device)
            store.commit('t1', {1: {'v': 1}, 2: {'v': 1}})
            store.checkpoint()
            store.stage('t2', {2: {'v': 2}, 3: {'v': 2}}, flush_pages=True)
            store.commit('t2')
            store.checkpoint()
            store.commit('t3', {1: {'v': 3}})
            return store

        probe = Device()
        build(probe)
        for fail_at in range(1, len(probe.trace) + 1):
            with self.subTest(fail_at=fail_at):
                victim = Device(fail_at=fail_at)
                try:
                    build(victim)
                    image = deepcopy(victim.durable)
                except PowerLoss:
                    image = victim.crash().durable
                first = Store.open(Device(deepcopy(image)))
                second = Store.open(Device(deepcopy(first.device.durable)))
                committed = set(first.committed_requests)
                # 恢复可重入：两次恢复的回执摘要完全一致。
                self.assertEqual(
                    {rid: first.receipt(rid)['digest'] for rid in committed},
                    {rid: second.receipt(rid)['digest']
                     for rid in second.committed_requests})
                # 每页当前值等于其最后一个已提交写入者。
                for pid, chain in page_chain.items():
                    expected = None
                    for rid, value in chain:
                        if rid in committed:
                            expected = value
                    self.assertEqual(first.read(pid)['value'], expected)
                # t2 的两页原子生效或原子缺席。
                t2_on = 't2' in committed
                self.assertEqual(first.read(2)['value'] == {'v': 2}, t2_on)
                self.assertEqual(first.read(3)['value'] == {'v': 2}, t2_on)
                self.assertEqual(first.device.read(WAL_TMP_NAME), b'')


if __name__ == '__main__':
    unittest.main()
