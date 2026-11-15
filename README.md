# 湖仓页存储的事务日志恢复与持久提交边界

frames.py 定义顺序日志帧，device.py 区分待同步字节与持久字节，pages.py 保存带页号和 LSN 的正文。
store.py 在三者之上实现跨页事务：WAL 提交、回执账、检查点截断与崩溃恢复，协议细节见 contracts.md。

运行环境为 Python 3.11。代码只使用标准库，数据位于 data/sample.json。
包内的 __main__.py 演示偷页掉电恢复、提交边界、重复请求与检查点截断。

```sh
python3 -m durablepages
python3 -m unittest discover -s tests -v
python3 -m compileall -q durablepages tests
```

## 公开接口速览

```python
from durablepages import Device, Store

store = Store.open(Device())                       # 打开即恢复
store.stage('req-1', {'1': {...}, '2': {...}},
            flush_pages=True)                      # 可选：提交前就把脏页 sync
receipt = store.commit('req-1')                    # 回执 sync 后才确认持久
store.commit('req-1', {'1': {...}, '2': {...}})    # 同身份同写集：幂等返回
store.abort('req-1')                               # 按 before 镜像撤销
store.checkpoint()                                 # 刷脏页并原子截断 WAL
store.read(1)                                      # 带页 LSN 的页内容
store.receipt('req-1')                             # 已确认回执或 None
```

测试使用临时目录与可注入失败点的 Device，在 write/sync/replace 边界逐点模拟掉电，
不改写项目内的数据。
