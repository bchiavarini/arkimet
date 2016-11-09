#include "arkiquery.h"
#include "arki/dataset/tests.h"
#include "arki/metadata/collection.h"

using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::runtime;
using namespace arki::utils;

namespace {

struct Fixture : public DatasetTest {
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        std::string config = R"(
            type = ondisk2
            step = daily
            index = origin, reftime
            unique = reftime, origin, product, level, timerange, area
        )";
        DatasetTest::test_setup(config);

        testdata::GRIBData fixture;
        wassert(import_all(fixture));
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} test("arki_runtime_arkiquery");

void Tests::register_tests() {

// Export only binary metadata (the default)
add_method("simple", [](Fixture& f) {
    ArkiQuery query;
    dataset::Reader::readConfig("testds", query.input_info);
    query.set_query("");
    query.set_output("test.md");
    query.main();

    wassert(actual_file("test.md").startswith("MD"));
    metadata::Collection mdc;
    mdc.read_from_file("test.md");
    wassert(actual(mdc.size()) == 3u);
});

// Export only data
add_method("simple", [](Fixture& f) {
    ArkiQuery query;
    dataset::Reader::readConfig("testds", query.input_info);
    query.set_query("");
    query.pmaker.data_only = true;
    query.set_output("test.grib");
    query.main();

    wassert(actual_file("test.grib").startswith("GRIB"));
    metadata::Collection mdc("test.grib");
    wassert(actual(mdc.size()) == 3u);
});

// Postprocess
add_method("postprocess", [](Fixture& f) {
    ArkiQuery query;
    dataset::Reader::readConfig("testds", query.input_info);
    query.set_query("");
    query.pmaker.postprocess = "checkfiles";
    setenv("ARKI_POSTPROC_FILES", "/dev/null", 1);
    query.set_output("test.out");
    query.main();

    wassert(actual(sys::read_file("test.out")) == "/dev/null\n");

    unsetenv("ARKI_POSTPROC_FILES");
});

}

}
