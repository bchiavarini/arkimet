#include <arki/dataset/tests.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/utils/sys.h>
#include <arki/utils/string.h>
#include "segment.h"
#include "segmented.h"
#include "segment/lines.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::dataset;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::tests;

class TestItem
{
public:
    std::string relname;

    TestItem(const std::string& relname) : relname(relname) {}
};

struct TestSegments
{
    ConfigFile cfg;
    std::string pathname;
    //const testdata::Fixture& td;

    /**
     * cfg_segments can be:
     *  - dir: directory segments
     *  - (nothing): AutoSegmentManager
     */
    TestSegments(const std::string& cfg_segments=std::string())
         : pathname("testsegment")
    {
        if (sys::isdir(pathname))
            sys::rmtree(pathname);
        else
            sys::unlink_ifexists(pathname);

        cfg.setValue("name", "test");
        cfg.setValue("step", "daily");
        cfg.setValue("path", pathname);
        if (!cfg_segments.empty())
            cfg.setValue("segments", cfg_segments);
    }

    void test_repack()
    {
        ConfigFile cfg1(cfg);
        cfg1.setValue("mockdata", "true");
        auto config = dataset::segmented::Config::create(cfg1);
        auto segment_manager = config->create_segment_manager();

        // Import 2 gigabytes of data in a single segment
        metadata::Collection mds;
        Segment* w = segment_manager->get_segment("test.grib");
        for (unsigned i = 0; i < 2048; ++i)
        {
            unique_ptr<Metadata> md(new Metadata(testdata::make_large_mock("grib", 1024*1024, i / (30 * 24), (i/24) % 30, i % 24)));
            unique_ptr<types::Source> old_source(md->source().clone());
            off_t ofs = w->append(*md);
            //wassert(actual(md->source().style()) == Source::BLOB);
            // Source does not get modified
            wassert(actual_type(md->source()) == *old_source);
            md->set_source(types::Source::createBlob(md->source().format, cfg.value("path"), w->relname, ofs, md->data_size()));
            md->drop_cached_data();
            mds.acquire(move(md));
        }

        // Repack it
        wassert(segment_manager->repack("test.grib", mds));
    }
};

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_dataset_segment");

void Tests::register_tests() {

// Test the item cache
add_method("item_cache", [] {
    segment::impl::Cache<TestItem, 3> cache;

    // Empty cache does not return things
    wassert(actual(cache.get("foo") == 0).istrue());

    // Add returns the item we added
    TestItem* foo = new TestItem("foo");
    wassert(actual(cache.add(unique_ptr<TestItem>(foo)) == foo).istrue());

    // Get returns foo
    wassert(actual(cache.get("foo") == foo).istrue());

    // Add two more items
    TestItem* bar = new TestItem("bar");
    wassert(actual(cache.add(unique_ptr<TestItem>(bar)) == bar).istrue());
    TestItem* baz = new TestItem("baz");
    wassert(actual(cache.add(unique_ptr<TestItem>(baz)) == baz).istrue());

    // With max_size=3, the cache should hold them all
    wassert(actual(cache.get("foo") == foo).istrue());
    wassert(actual(cache.get("bar") == bar).istrue());
    wassert(actual(cache.get("baz") == baz).istrue());

    // Add an extra item: the last recently used was foo, which gets popped
    TestItem* gnu = new TestItem("gnu");
    wassert(actual(cache.add(unique_ptr<TestItem>(gnu)) == gnu).istrue());

    // Foo is not in cache anymore, bar baz and gnu are
    wassert(actual(cache.get("foo") == 0).istrue());
    wassert(actual(cache.get("bar") == bar).istrue());
    wassert(actual(cache.get("baz") == baz).istrue());
    wassert(actual(cache.get("gnu") == gnu).istrue());
});

add_method("file", [] {
    TestSegments ts;
    wassert(ts.test_repack());
});

add_method("dir", [] {
    TestSegments ts("dir");
    wassert(ts.test_repack());
});

}

}