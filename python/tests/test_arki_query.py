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


#// Export only data
#add_method("simple", [](Fixture& f) {
#    ArkiQuery query;
#    dataset::Reader::readConfig("testds", query.input_info);
#    query.set_query("");
#    query.pmaker.data_only = true;
#    query.set_output("test.grib");
#    query.main();
#
#    wassert(actual_file("test.grib").startswith("GRIB"));
#    metadata::Collection mdc("test.grib");
#    wassert(actual(mdc.size()) == 3u);
#});
#
#// Postprocess
#add_method("postprocess", [](Fixture& f) {
#    ArkiQuery query;
#    dataset::Reader::readConfig("testds", query.input_info);
#    query.set_query("");
#    query.pmaker.postprocess = "checkfiles";
#    setenv("ARKI_POSTPROC_FILES", "/dev/null", 1);
#    query.set_output("test.out");
#    query.main();
#
#    wassert(actual(sys::read_file("test.out")) == "/dev/null\n");
#
#    unsetenv("ARKI_POSTPROC_FILES");
#});
#
