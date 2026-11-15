"""长度与校验保护的日志帧。"""
import json
import struct
import zlib

def encode(record):
    body = json.dumps(record, sort_keys=True, separators=(',', ':')).encode()
    return struct.pack('>II', len(body), zlib.crc32(body)) + body

def scan(raw):
    result = []
    offset = 0
    while offset < len(raw):
        if len(raw) - offset < 8:
            return result, offset, True
        length, checksum = struct.unpack('>II', raw[offset:offset + 8])
        if length > 16 * 1024 * 1024:
            raise ValueError('日志帧长度异常')
        end = offset + 8 + length
        if end > len(raw):
            return result, offset, True
        body = raw[offset + 8:end]
        if zlib.crc32(body) != checksum:
            raise ValueError('完整日志帧损坏')
        result.append(json.loads(body))
        offset = end
    return result, offset, False
