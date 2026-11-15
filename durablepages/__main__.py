"""端到端演示：事务提交边界、偷页、检查点与掉电恢复。

使用可注入失败点的公开存储替身 Device，核对中断前后的可见页与回执，
数据取自 data/sample.json。
"""
import json
import os

from .device import Device, PowerLoss
from .store import CorruptStorage, Store

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
with open(os.path.join(HERE, 'data', 'sample.json'), encoding='utf-8') as fh:
    SAMPLE = json.load(fh)


def show(title, store):
    visible = {str(pid): store.read(pid)['value'] for pid in (1, 2)}
    print(title, json.dumps({
        'pages': visible,
        'receipts': store.committed_requests,
        'next_lsn': store.next_lsn,
    }, ensure_ascii=False))


def main():
    request = SAMPLE['transaction']
    writes = {pid: payload for pid, payload in request['writes'].items()}
    rid = request['request_id']

    # 1. 页在提交前就 sync 落盘（偷页），随后在回执 sync 前掉电。
    device = Device()
    store = Store.open(device)
    store.stage(rid, writes, flush_pages=True)
    pre_commit_trace = len(device.trace)
    device.fail_at = pre_commit_trace + 1
    try:
        store.commit(rid)
    except PowerLoss:
        pass
    print('提交回执 sync 前掉电 -> 恢复')
    recovered = Store.open(device.crash())
    show('恢复后（未确认事务必须消失）:', recovered)
    assert recovered.receipt(rid) is None
    assert recovered.read(1)['value'] is None
    assert recovered.read(2)['value'] is None

    # 2. 完整提交：只有回执也 sync 后才返回确认。
    receipt = recovered.commit(rid, writes)
    print('重新提交回执:', json.dumps(receipt, ensure_ascii=False,
                                     sort_keys=True))
    after_commit = Device(recovered.device.durable)
    reopened = Store.open(after_commit)
    show('重新打开仓库（已确认事务必须保留）:', reopened)

    # 3. 重复请求：同内容幂等；同身份异写集拒绝。
    again = reopened.commit(rid, writes)
    print('重复请求幂等回执一致:', again['digest'] == receipt['digest'])
    try:
        reopened.commit(rid, {'1': {'rows': 999}})
    except ValueError as exc:
        print('同身份异写集拒绝:', type(exc).__name__)

    # 4. 检查点截断日志后再掉电，已确认事务不丢。
    reopened.checkpoint()
    print('检查点后 WAL 帧数:',
          len(__import__('durablepages.frames', fromlist=['scan']).scan(
              reopened.device.read('wal'))[0]))
    final = Store.open(Device(reopened.device.durable))
    show('检查点后重开:', final)
    assert final.receipt(rid) is not None


if __name__ == '__main__':
    main()
