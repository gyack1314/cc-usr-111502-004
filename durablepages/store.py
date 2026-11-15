"""跨页事务：WAL 提交、检查点与崩溃恢复。

持久布局（全部经由 :class:`~durablepages.device.Device`）：

* ``wal`` —— 顺序日志，帧格式见 :mod:`durablepages.frames`，记录类型
  ``begin / update / commit / abort / checkpoint``。
* ``receipts`` —— 提交回执账，每帧一个已确认请求，独立于 WAL 累积，
  检查点截断 WAL 后请求身份仍然可查。
* ``page/<page_id>`` —— 带页 LSN 的数据页（沿用 :mod:`durablepages.pages`）。
* ``wal.tmp`` —— 检查点重写日志时的临时文件，sync 后原子替换 ``wal``。

提交边界：只有 ``commit`` 帧所在的 WAL 与回执帧都 sync 完成后，才向上游
返回回执。页允许在事务提交前写出（偷页），但页文件 sync 之前对应日志记录
必须已经 sync（先写日志规则），因此恢复总能依据 update 帧里的 before 镜像
撤销未提交事务。
"""
import json

from .frames import encode, scan
from .pages import Pages

WAL_NAME = 'wal'
RECEIPTS_NAME = 'receipts'
WAL_TMP_NAME = 'wal.tmp'


class CorruptStorage(ValueError):
    """持久内容发生不可自动处理的损坏。

    与“事务尚未提交”是不同结论：恢复过程中遇到完整帧校验失败、页内容损坏
    等情况时抛出，绝不通过清空目录来掩盖。
    """


class ConflictingRequest(ValueError):
    """同一请求身份携带了与既有回执不同的写集。"""


def _canonical(writes):
    return json.dumps(writes, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False)


def _page_name(page_id):
    return 'page/' + str(page_id)


def _as_page_id(page_id):
    """页号归一化：纯数字字符串/正整数归一为 int，其余接受为字符串名。"""
    if isinstance(page_id, bool) or not isinstance(page_id, (int, str)):
        raise ValueError('页号必须是整数或非空字符串')
    if isinstance(page_id, int):
        if page_id <= 0:
            raise ValueError('整数页号必须为正')
        return page_id
    page_id = page_id.strip()
    if not page_id:
        raise ValueError('页号不能为空字符串')
    if page_id.isdigit():
        value = int(page_id)
        if value <= 0:
            raise ValueError('整数页号必须为正')
        return value
    return page_id


class Store:
    """单写者事务存储，串行提交。"""

    def __init__(self, device):
        self.device = device
        self.pages = Pages(device)
        self._log = []                 # 已持久/待持久的日志记录（内存镜像）
        self._receipts = {}            # request_id -> 回执 dict
        self._staged = {}              # request_id -> 活动事务上下文
        self._dirty = set()            # 待刷盘页号
        self._page_ids = set()         # 已知页号
        self._next_lsn = 1

    # ------------------------------------------------------------------
    # 打开与恢复
    # ------------------------------------------------------------------
    @classmethod
    def open(cls, device):
        """在一个（可能刚经历掉电的）设备上执行恢复并返回可用存储。"""
        store = cls(device)
        store._recover()
        return store

    def _recover(self):
        # 上一轮检查点可能在 replace 之前掉电：未发布的临时日志一律清除，
        # 正式 wal 仍是唯一权威。
        if self.device.read(WAL_TMP_NAME):
            self.device.write(WAL_TMP_NAME, b'')
            self.device.sync(WAL_TMP_NAME)
        # 设备上实际存在的页全部纳入页号集合：新页可能已偷写落盘但对应
        # 事务尚未提交（WAL 里同样有它的 update 帧，这里先兜底）。
        # lsn=0 的墓碑帧表示“页不存在”，不纳入；损坏页在此即被识别。
        for name in self.device.pending:
            if name.startswith('page/'):
                suffix = name[len('page/'):]
                if suffix and self.device.pending[name]:
                    page = self._read_page(suffix)
                    if page['lsn'] > 0:
                        self._page_ids.add(_as_page_id(suffix))
        records = self._scan_durable(WAL_NAME)
        receipt_frames = self._scan_durable(RECEIPTS_NAME)

        # 回执账先入账：检查点可能已经截掉对应的 commit 记录。
        for frame in receipt_frames:
            rid = frame['request_id']
            prior = self._receipts.get(rid)
            if prior is not None and prior['digest'] != _canonical(frame['writes']):
                raise CorruptStorage('回执账中同一请求身份的写集不一致')
            self._receipts[rid] = {
                'request_id': rid,
                'status': 'committed',
                'lsn': frame['lsn'],
                'writes': frame['writes'],
                'digest': _canonical(frame['writes']),
            }

        states = {}
        max_lsn = 0
        for rec in records:
            max_lsn = max(max_lsn, rec.get('lsn', 0))
            kind = rec['kind']
            if kind == 'checkpoint':
                states.update({txn: 'active' for txn in rec.get('active', [])})
                self._page_ids.update(rec.get('pages', []))
            elif kind == 'begin':
                states.setdefault(rec['txn'], 'active')
            elif kind == 'update':
                states.setdefault(rec['txn'], 'active')
                self._page_ids.add(rec['page_id'])
            elif kind in ('commit', 'abort'):
                states[rec['txn']] = kind

        # 先校验所有已知页，损坏按 CorruptStorage 上报，不得清空掩盖。
        for pid in sorted(self._page_ids):
            self._read_page(pid)

        # REDO：只重放最终已提交事务的 update，按 LSN 顺序、页 LSN 落后
        # 才写，天然幂等。未提交与已 abort 事务的 update 一律不重放：
        # 前者由下面的 UNDO 撤销，后者的页还原动作本身就是终态。
        committed = {txn for txn, state in states.items()
                     if state == 'commit'}
        for rec in records:
            if rec['kind'] != 'update' or rec['txn'] not in committed:
                continue
            pid = rec['page_id']
            current = self._read_page(pid)
            after = rec['after']
            if current['lsn'] < after['lsn']:
                self.pages.write(pid, after['lsn'], after['value'], flush=True)
            self._page_ids.add(pid)

        # UNDO：没有 commit/abort 结论的事务，逆序还原 before 镜像。
        # before 是该 update 应用前的精确镜像，直接覆盖即可；重放恢复
        # （含恢复中断后再恢复）时幂等：同一事务的 before 链逆序收敛。
        active_txns = [txn for txn, state in states.items() if state == 'active']
        for txn in active_txns:
            updates = [r for r in records
                       if r['kind'] == 'update' and r['txn'] == txn]
            for rec in reversed(updates):
                pid = rec['page_id']
                before = rec['before']
                if before is None:
                    self._write_tombstone(pid)
                else:
                    self.pages.write(pid, before['lsn'], before['value'],
                                     flush=True)
            self._append({'kind': 'abort', 'lsn': self._alloc_lsn(),
                          'txn': txn})
            states[txn] = 'abort'
        if active_txns:
            self.device.sync(WAL_NAME)
        # UNDO 可能把新建页还原成墓碑（lsn=0），按设备上的实际页文件
        # 重新收敛页号集合，墓碑不占页号。
        self._page_ids = {
            pid for pid in self._page_ids
            if self._read_page(pid)['lsn'] > 0
        }
        # 回执补偿：commit 已持久、回执帧未及 sync 的窗口由此闭合。
        for rec in records:
            if rec['kind'] != 'commit':
                continue
            rid = rec['txn']
            if rid not in self._receipts:
                self._append_receipt(rid, rec['lsn'], rec['writes'])
                self.device.sync(RECEIPTS_NAME)
            self._receipts.setdefault(rid, {
                'request_id': rid,
                'status': 'committed',
                'lsn': rec['lsn'],
                'writes': rec['writes'],
                'digest': _canonical(rec['writes']),
            })

        self._next_lsn = max(max_lsn, self._max_page_lsn()) + 1
        self._log = list(records)
        self._dirty.clear()  # 恢复期间所有页写均已 sync。

    def _scan_durable(self, name):
        """扫描帧文件。

        尾部撕裂（最后一帧不完整）：截掉撕裂字节并持久化截断结果；
        中间完整帧损坏：抛 :class:`CorruptStorage`，两者严格区分。
        """
        raw = self.device.read(name)
        try:
            records, consumed, torn = scan(raw)
        except ValueError as exc:
            raise CorruptStorage(name + ' 日志帧损坏: ' + str(exc)) from exc
        if torn:
            self.device.write(name, raw[:consumed])
            self.device.sync(name)
        return records

    # ------------------------------------------------------------------
    # 事务接口
    # ------------------------------------------------------------------
    def stage(self, request_id, writes, *, flush_pages=False):
        """登记一个多页事务，可选择把脏页在提交前就 sync 落盘。"""
        if not isinstance(request_id, str) or not request_id:
            raise ValueError('request_id 必须是非空字符串')
        if not isinstance(writes, dict) or not writes:
            raise ValueError('写集必须是非空映射')
        if request_id in self._receipts:
            raise ValueError('请求已提交，重复请求请走 commit 接口')
        if self._staged:
            raise ValueError('当前存在未结束的事务，业务串行提交')
        normalized = {_as_page_id(pid): value for pid, value in writes.items()}

        self._append({'kind': 'begin', 'lsn': self._alloc_lsn(),
                      'txn': request_id})
        frames = []
        for pid, value in normalized.items():
            current = self._read_page(pid)
            before = None if current['lsn'] == 0 else dict(current)
            rec = {
                'kind': 'update',
                'lsn': self._alloc_lsn(),
                'txn': request_id,
                'page_id': pid,
                'before': before,
                'after': {'lsn': None, 'value': value},
            }
            rec['after']['lsn'] = rec['lsn']
            self._append(rec)
            frames.append(rec)
            self._page_ids.add(pid)
        if flush_pages:
            # 先写日志规则：全部 before/after 镜像持久之后才允许页提前落盘。
            self.device.sync(WAL_NAME)
        for rec in frames:
            if flush_pages:
                self.pages.write(rec['page_id'], rec['lsn'],
                                 rec['after']['value'], flush=True)
            else:
                self.pages.write(rec['page_id'], rec['lsn'],
                                 rec['after']['value'])
            self._dirty.add(rec['page_id'])
        self._staged[request_id] = {'writes': normalized, 'frames': frames}
        return request_id

    def commit(self, request_id, writes=None, *, flush_pages=False):
        """提交事务并返回回执；重复请求按身份幂等处理。

        * 同身份同写集：直接返回既有回执，不产生任何写入。
        * 同身份异写集：抛 :class:`ConflictingRequest`。
        """
        if writes is not None:
            normalized = {_as_page_id(pid): value for pid, value in writes.items()}
        if request_id in self._receipts:
            receipt = self._receipts[request_id]
            if writes is None or receipt['digest'] == _canonical(normalized):
                return dict(receipt)
            raise ConflictingRequest('请求身份已确认但写集不同')

        if request_id not in self._staged:
            if writes is None:
                raise ValueError('未知事务且未提供写集')
            self.stage(request_id, normalized, flush_pages=flush_pages)
        elif writes is not None:
            staged = self._staged[request_id]
            if _canonical(staged['writes']) != _canonical(normalized):
                raise ConflictingRequest('请求身份已暂存但写集不同')

        staged = self._staged.pop(request_id)
        commit_lsn = self._alloc_lsn()
        self._append({'kind': 'commit', 'lsn': commit_lsn,
                      'txn': request_id, 'writes': staged['writes']})
        self.device.sync(WAL_NAME)          # 持久提交点：commit 帧落盘
        # 确认前把该事务的页全部推到 durable。WAL 已先持久，满足先写日志；
        # 这样“已确认 ⇒ 对应页版本持久”，检查点才能安全截断其日志前缀。
        for rec in staged['frames']:
            pid = rec['page_id']
            if pid in self._dirty:
                self.pages.write(pid, rec['lsn'], rec['after']['value'],
                                 flush=True)
                self._dirty.discard(pid)
        self._append_receipt(request_id, commit_lsn, staged['writes'])
        self.device.sync(RECEIPTS_NAME)     # 回执持久后才可向上游确认
        receipt = {
            'request_id': request_id,
            'status': 'committed',
            'lsn': commit_lsn,
            'writes': staged['writes'],
            'digest': _canonical(staged['writes']),
        }
        self._receipts[request_id] = receipt
        return dict(receipt)

    def abort(self, request_id):
        """主动撤销事务，页还原并写 abort 记录。

        先把还原所需信息（update 帧的 before 镜像）随 abort 帧一起 sync，
        再 sync 还原后的页，任何中断点恢复都能得到一致结论。
        """
        staged = self._staged.pop(request_id)
        self._append({'kind': 'abort', 'lsn': self._alloc_lsn(),
                      'txn': request_id})
        self.device.sync(WAL_NAME)
        for rec in reversed(staged['frames']):
            pid = rec['page_id']
            current = self._read_page(pid)
            if current['lsn'] != rec['after']['lsn']:
                continue
            before = rec['before']
            if before is None:
                self._write_tombstone(pid)
            else:
                self.pages.write(pid, before['lsn'], before['value'],
                                 flush=True)

    def checkpoint(self):
        """刷脏页并截断日志前缀。

        顺序：sync WAL → sync 全部脏页 → 写 ``wal.tmp`` → sync 临时文件 →
        原子替换。替换前任意时刻崩溃，旧 WAL 仍完整可恢复；活动事务的
        日志记录（含 before 镜像）整体保留，不参与截断。
        """
        self.device.sync(WAL_NAME)
        for pid in sorted(self._dirty):
            self.device.sync(_page_name(pid))
        self._dirty.clear()

        active = set(self._staged)
        cutoff = 0
        for idx, rec in enumerate(self._log):
            if rec['kind'] in ('begin', 'update') and rec['txn'] in active:
                cutoff = idx
                break
        else:
            cutoff = len(self._log)
        kept = self._log[cutoff:]

        marker = {
            'kind': 'checkpoint',
            'lsn': self._alloc_lsn(),
            'pages': sorted(self._page_ids),
            'active': sorted(active),
        }
        payload = encode(marker) + b''.join(encode(rec) for rec in kept)
        self.device.write(WAL_TMP_NAME, payload)
        self.device.sync(WAL_TMP_NAME)
        self.device.replace(WAL_TMP_NAME, WAL_NAME)
        self._log = [marker] + kept

    def read(self, page_id):
        """读页（进程内可见未提交修改；重开后未提交修改已被撤销）。"""
        page_id = _as_page_id(page_id)
        page = self._read_page(page_id)
        self._page_ids.add(page_id)
        return page

    def receipt(self, request_id):
        """返回已确认回执，未确认返回 None。"""
        receipt = self._receipts.get(request_id)
        return None if receipt is None else dict(receipt)

    @property
    def next_lsn(self):
        return self._next_lsn

    @property
    def committed_requests(self):
        return sorted(self._receipts)

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------
    def _alloc_lsn(self):
        lsn = self._next_lsn
        self._next_lsn += 1
        return lsn

    def _append(self, record):
        self.device.append(WAL_NAME, encode(record))
        self._log.append(record)

    def _append_receipt(self, request_id, lsn, writes):
        frame = {'request_id': request_id, 'lsn': lsn, 'writes': writes}
        self.device.append(RECEIPTS_NAME, encode(frame))

    def _read_page(self, page_id):
        try:
            return self.pages.read(page_id)
        except ValueError as exc:
            if isinstance(exc, CorruptStorage):
                raise
            raise CorruptStorage('页 %s 内容损坏: %s' % (page_id, exc)) from exc

    def _write_tombstone(self, page_id):
        """把页还原为“不存在”语义（页曾被未提交事务创建）。"""
        name = _page_name(page_id)
        self.device.write(name, encode(
            {'page_id': page_id, 'lsn': 0, 'value': None}))
        self.device.sync(name)
        self._page_ids.discard(page_id)
        self._dirty.discard(page_id)

    def _max_page_lsn(self):
        maximum = 0
        for pid in self._page_ids:
            maximum = max(maximum, self._read_page(pid)['lsn'])
        return maximum
