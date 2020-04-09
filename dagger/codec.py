"""
payload length      (32bit)
sequence number     (16bit)
event type          (4bit)
compress flag       (1bit)
error number        (3bit)
magic               (8bit)  72
"""
import enum
import io
from datetime import datetime, date
from typing import NamedTuple, ByteString

from brotli import compress, decompress
from msgpack import loads, dumps, ExtType

from dagger.exceptions import FrameError, PackUnpackError
from dagger.datetimeutils import date2int8, datetime2int14, int8_to_date, int14_to_datetime

try:
    import pandas as pd
except ImportError:
    pandas_support = False
else:
    pandas_support = True

try:
    import numpy as np
except ImportError:
    numpy_support = False
else:
    numpy_support = True

__all__ = (
    "EventType",
    "decode_header",
    "encode_header",
    "pack_message",
    "unpack_payload",
    "Header",
    "MAX_SEQUENCE_ID",
)

MAX_SEQUENCE_ID = 2 ** 16 - 1


class EventType(enum.IntEnum):
    REQUEST = 1
    RESPONSE = 2
    AUTH = 3


class Header(NamedTuple):
    payload_size: int
    sequence_number: int
    compress_flag: int
    errno: int
    event_type: int


def decode_header(buffer: ByteString) -> Header:
    assert len(buffer) == 8
    header = int.from_bytes(buffer, "big")
    magic = header & 255
    if magic != 72:
        raise FrameError(f"invalid magic number: {magic}")

    payload_size = header >> 32
    seq = (header & 4294901760) >> 16
    event_type = (header & 61440) >> 12
    compress_flag = (header & 2048) >> 11
    error_no = (header & 1792) >> 8
    return Header(payload_size, seq, compress_flag, error_no, event_type)


def encode_header(
    payload_size: int, seq_id: int, compress_flag: int, error_no: int, event_type: int
) -> bytes:
    rv = payload_size << 32
    rv |= seq_id << 16
    rv |= event_type << 12
    rv |= compress_flag << 11
    rv |= error_no << 8
    rv |= 72
    return rv.to_bytes(8, "big", signed=False)


def unpack_payload(compress_flag: int, data: bytes) -> object:
    try:
        if compress_flag:
            data = decompress(data)

        return loads(
            data,
            use_list=True,
            raw=False,
            ext_hook=_ext_hook,
            max_str_len=2147483647,  # 2**32-1
            max_bin_len=2147483647,
            max_array_len=2147483647,
            max_map_len=2147483647,
            max_ext_len=2147483647,
        )
    except Exception as e:
        raise PackUnpackError(e)


def pack_message(seq_id: int, event_type: int, ob) -> bytes:
    """pack up on message, set errno by default"""
    try:
        if isinstance(ob, Exception):
            error_no = getattr(ob, "code", 500)  # 500 -> RemoteIntervalError
            ob = str(ob)
        else:
            error_no = 0

        data = dumps(ob, use_bin_type=True, default=_default)
        if len(data) > 1024:
            data = compress(data)
            compress_flag = 1
        else:
            compress_flag = 0
        header = encode_header(len(data), seq_id, compress_flag, error_no, event_type)
        return header + data
    except Exception as e:
        raise PackUnpackError(e)


_EPOCH = datetime(1970, 1, 1)


class _ExtDefine(NamedTuple):
    pandas_dataframe: int = 1
    numpy_array: int = 3
    date: int = 5
    datetime: int = 6


EXT_TYPES = _ExtDefine()


def _ext_hook(code: int, data: bytes):
    if code == EXT_TYPES.datetime:
        return int14_to_datetime(int.from_bytes(data, "big", signed=True))
    elif code == EXT_TYPES.date:
        return int8_to_date(int.from_bytes(data, "big", signed=True))
    elif pandas_support and code == EXT_TYPES.pandas_dataframe:
        return bytes2dataframe(data)
    if numpy_support and code == EXT_TYPES.numpy_array:
        return bytes2array(data)
    else:
        return ExtType(code, data)


def _default(obj):
    if isinstance(obj, datetime):
        return ExtType(EXT_TYPES.datetime, datetime2int14(obj).to_bytes(6, "big", signed=True))
    elif isinstance(obj, date):
        return ExtType(EXT_TYPES.date, date2int8(obj).to_bytes(4, "big", signed=True))
    elif pandas_support and isinstance(obj, pd.DataFrame):
        return ExtType(EXT_TYPES.pandas_dataframe, dataframe2bytes(obj))
    elif numpy_support and isinstance(obj, np.ndarray):
        return ExtType(EXT_TYPES.numpy_array, array2bytes(obj))
    else:
        raise TypeError(f"Unknown type: {type(obj)}")


def array2bytes(array) -> bytes:
    header = bytearray()
    header.extend(b"#type:ndarray\n")
    sp = ",".join(str(i) for i in array.shape)
    header.extend(b"#shape:")
    header.extend(sp.encode())
    header.extend(b"\n")
    dtype = array.dtype
    header.extend(b"#dtype:")
    if len(dtype) != 0:
        dtypes = []
        for k, (v, _) in dtype.fields.items():
            if v.name == "object":
                raise TypeError("don't support numpy object dtype")
            dtypes.append(k)
            dtypes.append(v.name)
        header.extend(",".join(dtypes).encode())
    else:
        header.extend(dtype.name.encode())
    header.extend(b"\n")
    return bytes(header) + array.tobytes()


def dataframe2bytes(array) -> bytes:
    header = bytearray()
    df = array.reset_index()
    header.extend(b"#type:dataframe\n")
    header.extend(b"#dtype:")
    dtypes = []
    for k, v in df.dtypes.items():
        dtypes.append(k)
        dtypes.append(v.name)
    header.extend(",".join(dtypes).encode())
    header.extend(b"\n")
    if len(array.index.names) != 1:
        raise TypeError(f"don't support multi index dataframe")
    if not array.index.name:
        idx_name = "index"
    else:
        idx_name = array.index.name
    header.extend(b"#index:")
    header.extend(idx_name.encode())
    header.extend(b"\n")
    return bytes(header) + df.to_csv(header=True, index=False).encode()


def _zipme(it, n=2):
    tp = []
    it = iter(it)
    exhausted = False
    while not exhausted:
        for i in range(n):
            try:
                item = next(it)
            except StopIteration:
                exhausted = True
            else:
                tp.append(item)
        if tp:
            yield tuple(tp)
            tp.clear()


def bytes2array(data: bytes) -> "np.ndarray":
    buffer = io.BytesIO(data)
    type_ = buffer.readline()[6:-1].decode()
    if type_ != "ndarray":
        raise TypeError(f"invalid type string: {type_}")
    shape = buffer.readline()[7:-1].decode()
    dtype = buffer.readline()[7:-1].decode()
    dtype = dtype.split(",")
    if len(dtype) == 1:
        dtype = np.dtype(dtype[0])
    else:
        dtype = list(_zipme(dtype, 2))
        dtype = np.dtype(dtype)
    shape = tuple(int(i) for i in shape.split(","))
    array = np.frombuffer(memoryview(data)[buffer.tell() :], dtype=dtype)
    array.shape = shape
    return array


def bytes2dataframe(data: bytes) -> "pd.DataFrame":
    buffer = io.BytesIO(data)
    type_ = buffer.readline()[6:-1].decode()
    if type_ != "dataframe":
        raise TypeError(f"invalid type string: {type_}")

    dtype = buffer.readline()[7:-1].decode()
    dtype = dtype.split(",")
    date_col = []
    if len(dtype) == 1:
        dtypes = dtype[0]
    else:
        dtypes = {}
        for name, dtype in _zipme(dtype, 2):
            if np.issubdtype(dtype, np.datetime64):
                date_col.append(name)
            dtypes[name] = dtype

    index = buffer.readline()[7:-1].decode()

    array = pd.read_csv(buffer, index_col=False, header=0)
    for col in date_col:
        array[col] = pd.to_datetime(array[col])
    array = array.astype(dtypes, copy=False)
    array.set_index(index, inplace=True)
    return array
