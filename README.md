# 湖仓页存储的事务日志恢复与持久提交边界

frames.py 定义顺序日志帧，device.py 区分待同步字节与持久字节，pages.py 保存带页号和 LSN 的正文。现有组件没有跨页事务、恢复协调器或提交回执账。

运行环境为 Python 3.11。代码只使用标准库，数据位于 data/sample.json。
包内的 __main__.py 运行现有组件并输出其实际处理结果。

```sh
python3 -m durablepages
python3 -m unittest discover -s tests -v
python3 -m compileall -q durablepages tests
```

文件职责与数据约定见 contracts.md。测试使用临时目录，不改写项目内的数据。
