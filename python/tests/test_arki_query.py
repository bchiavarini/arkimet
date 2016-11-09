import unittest
import tempfile
import arkimet as arki


class TestArkiQuery(unittest.TestCase):
    input_cfg = {
        "test": {
            "format": "grib",
            "name": "test.grib1",
            "path": "inbound/test.grib1",
            "type": "file",
            },
    }

    def test_simple(self):
        arki.arki_query(
            query="",
            input=self.input_cfg,
            output="test.md"
        )

        with open("test.md", "rb") as fd:
            data = fd.read()
            self.assertGreater(len(data), 2)
            self.assertEqual(data[:2], b"MD")

    def test_data(self):
        arki.arki_query(
            query="",
            input=self.input_cfg,
            output="test.grib",
            data_only=True,
        )

        with open("test.grib", "rb") as fd:
            data = fd.read()
            self.assertGreater(len(data), 8)
            self.assertEqual(data[:4], b"GRIB")
            self.assertEqual(data[-4:], b"7777")

    def test_postprocess(self):
        arki.arki_query(
            query="",
            input=self.input_cfg,
            output="test.out",
            postprocess="checkfiles",
            postproc_data="/dev/null"
        )

        with open("test.out", "rt") as fd:
            data = fd.read()
            self.assertEquals(data, "/dev/null\n")
