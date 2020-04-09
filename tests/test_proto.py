import random
import unittest
import datetime

from dagger.codec import decode_header, encode_header, EventType, pack_message, unpack_payload


class TestProto(unittest.TestCase):
    def test_decode_and_encode_header(self):
        max = 2 ** 32 - 1
        for i in range(10000):
            a = random.randint(0, max)
            d = encode_header(a, 1, 1, 1, EventType.REQUEST.value)
            h = decode_header(d)
            self.assertEqual(h.payload_size, a, msg=a)
            s = encode_header(*h)
            self.assertEqual(d, s, msg=a)

    def _packunpack(self, ob):
        data = pack_message(1, 1, ob)
        header = data[:8]
        body = data[8:]
        h = decode_header(header)
        return unpack_payload(h.compress_flag, body)

    def test_datetime(self):
        dt = datetime.datetime.now().replace(microsecond=0)
        un = self._packunpack(dt)
        self.assertEqual(dt, un)

    def test_date(self):
        dt = datetime.date.today()
        un = self._packunpack(dt)
        self.assertEqual(dt, un)

    def test_nparray(self):
        try:
            import numpy as np
        except ImportError:
            return

        arr = np.array(range(10), dtype=int)
        un = self._packunpack(arr)
        np.testing.assert_array_equal(arr, un)

        arr = np.empty((2, 3))
        un = self._packunpack(arr)
        np.testing.assert_array_equal(arr, un)

        arr = np.ndarray((3,), dtype=[("a", int), ("b", float), ("c", "M8")])
        un = self._packunpack(arr)
        np.testing.assert_array_equal(arr, un)

    def test_dataframe(self):
        try:
            import pandas as pd
        except ImportError:
            return

        df = pd.DataFrame(
            {
                "a": range(10),
                "b": pd.date_range("2017-01-01", periods=10, freq="D"),
                "c": [float(i) for i in range(10)],
            }
        )
        un = self._packunpack(df)
        pd.testing.assert_frame_equal(df, un, check_names=False)
