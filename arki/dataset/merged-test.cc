#include "arki/dataset/tests.h"
#include "arki/dataset/merged.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/matcher.h"
#include <sstream>
#include <iostream>

using namespace std;
using namespace arki;
using namespace arki::tests;

namespace {

struct Fixture : public arki::utils::tests::Fixture
{
    core::cfg::Sections config;
    dataset::Merged ds;

    Fixture()
    {
        // Cleanup the test datasets
        system("rm -rf test200/*");
        system("rm -rf test80/*");
        system("rm -rf error/*");

        // In-memory dataset configuration
        string conf =
            "[test200]\n"
            "type = ondisk2\n"
            "step = daily\n"
            "filter = origin: GRIB1,200\n"
            "index = origin, reftime\n"
            "name = test200\n"
            "path = test200\n"
            "\n"
            "[test80]\n"
            "type = ondisk2\n"
            "step = daily\n"
            "filter = origin: GRIB1,80\n"
            "index = origin, reftime\n"
            "name = test80\n"
            "path = test80\n"
            "\n"
            "[error]\n"
            "type = error\n"
            "step = daily\n"
            "name = error\n"
            "path = error\n";
        config = core::cfg::Sections::parse(conf, "(memory)");

        // Import data into the datasets
        metadata::TestCollection mdc("inbound/test.grib1");
        wassert(import("test200", mdc[0]));
        wassert(import("test80", mdc[1]));
        wassert(import("error", mdc[2]));
        ds.add_dataset(*config.section("test200"));
        ds.add_dataset(*config.section("test80"));
        ds.add_dataset(*config.section("error"));
    }

    void import(const std::string& dsname, Metadata& md)
    {
        auto writer = dataset::Config::create(*config.section(dsname))->create_writer();
        wassert(tests::actual(writer.get()).import(md));
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} tests("arki_dataset_merged");

void Tests::register_tests() {

// Test querying the datasets
add_method("query", [](Fixture& f) {
    metadata::Collection mdc(f.ds, Matcher());
    wassert(actual(mdc.size()) == 3u);
});

}

}
