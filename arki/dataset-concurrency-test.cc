#include "arki/dataset/tests.h"
#include "arki/dataset.h"
#include "arki/dataset/time.h"
#include "arki/dataset/reporter.h"
#include "arki/metadata/collection.h"
#include "arki/types/source.h"
#include "arki/types/source/blob.h"
#include "arki/types/reftime.h"
#include "arki/scan/any.h"
#include "arki/configfile.h"
#include "arki/utils/files.h"
#include "arki/utils/accounting.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/exceptions.h"
#include "wibble/sys/childprocess.h"
#include <sys/fcntl.h>
#include <iostream>

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

namespace {

template<class Data>
struct FixtureWriter : public DatasetTest
{
    Data td;

    FixtureWriter(const std::string& cfg_instance=std::string())
        : DatasetTest(R"(
            step = daily
            unique = product, area, reftime
        )" + cfg_instance) {}

    bool smallfiles() const
    {
        return ConfigFile::boolValue(cfg.value("smallfiles")) ||
            (td.format == "vm2" && cfg.value("type") == "simple");
    }
};


class TestSubprocess : public wibble::sys::ChildProcess
{
protected:
    int commfd;

public:
    void start()
    {
        forkAndRedirect(0, &commfd);
    }

    void notify_ready()
    {
        putchar('H');
        fflush(stdout);
    }

    char wait_until_ready()
    {
        char buf[2];
        ssize_t res = read(commfd, buf, 1);
        if (res < 0)
            throw_system_error("reading 1 byte from child process");
        if (res == 0)
            throw runtime_error("child process closed stdout without producing any output");
        return buf[0];
    }
};


template<class Fixture>
struct ConcurrentImporter : public wibble::sys::ChildProcess
{
    Fixture& fixture;
    unsigned initial;
    unsigned increment;

    ConcurrentImporter(Fixture& fixture, unsigned initial, unsigned increment)
        : fixture(fixture), initial(initial), increment(increment)
    {
    }

    virtual int main() override
    {
        auto ds(fixture.config().create_writer());

        Metadata md = fixture.td.test_data[0].md;

        for (unsigned i = initial; i < 60; i += increment)
        {
            md.set(types::Reftime::createPosition(core::Time(2016, 6, 1, 0, 0, i)));
            wassert(actual(ds->acquire(md)) == dataset::Writer::ACQ_OK);
        }

        return 0;
    }
};

struct ReadHang : public TestSubprocess
{
    std::shared_ptr<const dataset::Config> config;

    ReadHang(std::shared_ptr<const dataset::Config> config) : config(config) {}

    bool eat(unique_ptr<Metadata>&& md)
    {
        notify_ready();
        // Get stuck while reading
        while (true)
            usleep(100000);
        return true;
    }

    int main() override
    {
        try {
            auto reader = config->create_reader();
            reader->query_data(Matcher(), [&](unique_ptr<Metadata> md) { return eat(move(md)); });
        } catch (std::exception& e) {
            cerr << e.what() << endl;
            cout << "E" << endl;
            return 1;
        }
        return 0;
    }
};

struct HungReporter : public dataset::NullReporter
{
    TestSubprocess& sp;

    HungReporter(TestSubprocess& sp) : sp(sp) {}

    void segment_info(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        sp.notify_ready();
        while (true)
            sleep(3600);
    }
};

template<class Fixture>
struct CheckForever : public TestSubprocess
{
    Fixture& fixture;
    int commfd;

    CheckForever(Fixture& fixture) : fixture(fixture) {}

    int main() override
    {
        auto ds(fixture.config().create_checker());
        HungReporter reporter(*this);
        ds->check(reporter, false, false);
        return 0;
    }
};


template<class Data>
class Tests : public FixtureTestCase<FixtureWriter<Data>>
{
    using FixtureTestCase<FixtureWriter<Data>>::FixtureTestCase;

    void register_tests() override;
};

Tests<testdata::GRIBData> test_concurrent_grib_ondisk2("arki_dataset_concurrent_grib_ondisk2", "type=ondisk2\n");
Tests<testdata::GRIBData> test_concurrent_grib_simple_plain("arki_dataset_concurrent_grib_simple_plain", "type=simple\nindex_type=plain\n");
Tests<testdata::GRIBData> test_concurrent_grib_simple_sqlite("arki_dataset_concurrent_grib_simple_sqlite", "type=simple\nindex_type=sqlite");
Tests<testdata::GRIBData> test_concurrent_grib_iseg("arki_dataset_concurrent_grib_iseg", "type=iseg\nformat=grib\n");
Tests<testdata::BUFRData> test_concurrent_bufr_ondisk2("arki_dataset_concurrent_bufr_ondisk2", "type=ondisk2\n");
Tests<testdata::BUFRData> test_concurrent_bufr_simple_plain("arki_dataset_concurrent_bufr_simple_plain", "type=simple\nindex_type=plain\n");
Tests<testdata::BUFRData> test_concurrent_bufr_simple_sqlite("arki_dataset_concurrent_bufr_simple_sqlite", "type=simple\nindex_type=sqlite");
Tests<testdata::BUFRData> test_concurrent_bufr_iseg("arki_dataset_concurrent_bufr_iseg", "type=iseg\nformat=bufr\n");
Tests<testdata::VM2Data> test_concurrent_vm2_ondisk2("arki_dataset_concurrent_vm2_ondisk2", "type=ondisk2\n");
Tests<testdata::VM2Data> test_concurrent_vm2_simple_plain("arki_dataset_concurrent_vm2_simple_plain", "type=simple\nindex_type=plain\n");
Tests<testdata::VM2Data> test_concurrent_vm2_simple_sqlite("arki_dataset_concurrent_vm2_simple_sqlite", "type=simple\nindex_type=sqlite");
Tests<testdata::VM2Data> test_concurrent_vm2_iseg("arki_dataset_concurrent_vm2_iseg", "type=iseg\nformat=vm2\n");
Tests<testdata::ODIMData> test_concurrent_odim_ondisk2("arki_dataset_concurrent_odim_ondisk2", "type=ondisk2\n");
Tests<testdata::ODIMData> test_concurrent_odim_simple_plain("arki_dataset_concurrent_odim_simple_plain", "type=simple\nindex_type=plain\n");
Tests<testdata::ODIMData> test_concurrent_odim_simple_sqlite("arki_dataset_concurrent_odim_simple_sqlite", "type=simple\nindex_type=sqlite");
Tests<testdata::ODIMData> test_concurrent_odim_iseg("arki_dataset_concurrent_odim_iseg", "type=iseg\nformat=odimh5\n");

template<class Data>
void Tests<Data>::register_tests() {

typedef FixtureWriter<Data> Fixture;

this->add_method("read_read", [](Fixture& f) {
    f.import_all(f.td);

    // Query the index and hang
    ReadHang readHang(f.dataset_config());
    readHang.start();
    wassert(actual(readHang.wait_until_ready()) == 'H');

    // Query in parallel with the other read
    metadata::Collection mdc(*f.config().create_reader(), Matcher());

    readHang.kill(9);
    readHang.wait();

    wassert(actual(mdc.size()) == 3u);
});

// Test acquiring with a reader who's stuck on output
this->add_method("read_write", [](Fixture& f) {
    f.clean();

    // Import one grib in the dataset
    {
        auto ds = f.config().create_writer();
        wassert(actual(ds->acquire(f.td.test_data[0].md)) == dataset::Writer::ACQ_OK);
        ds->flush();
    }

    // Query the index and hang
    ReadHang readHang(f.dataset_config());
    readHang.start();
    wassert(actual(readHang.wait_until_ready()) == 'H');

    // Import another grib in the dataset
    {
        auto ds = f.config().create_writer();
        wassert(actual(ds->acquire(f.td.test_data[1].md)) == dataset::Writer::ACQ_OK);
        ds->flush();
    }

    readHang.kill(9);
    readHang.wait();

    metadata::Collection mdc1(*f.config().create_reader(), Matcher());
    wassert(actual(mdc1.size()) == 2u);
});

this->add_method("read_write1", [](Fixture& f) {
    f.reset_test("step=single");

    // Import one
    {
        auto writer = f.dataset_config()->create_writer();
        wassert(actual(writer->acquire(f.td.test_data[0].md)) == dataset::Writer::ACQ_OK);
    }

    // Query it and import during query
    auto reader = f.dataset_config()->create_reader();
    unsigned count = 0;
    reader->query_data(Matcher(), [&](unique_ptr<Metadata> md) {
        {
            // Make sure we only get one query result, that is, we don't read
            // the thing we import during the query
            wassert(actual(count) == 0u);

            auto writer = f.dataset_config()->create_writer();
            wassert(actual(writer->acquire(f.td.test_data[1].md)) == dataset::Writer::ACQ_OK);
            wassert(actual(writer->acquire(f.td.test_data[2].md)) == dataset::Writer::ACQ_OK);
        }
        ++count;
        return true;
    });
    wassert(actual(count) == 1u);

    // Querying again returns all imported data
    count = 0;
    reader->query_data(Matcher(), [&](unique_ptr<Metadata> md) { ++count; return true; });
    wassert(actual(count) == 3u);
});

this->add_method("write_write", [](Fixture& f) {
    ConcurrentImporter<Fixture> i0(f, 0, 3);
    ConcurrentImporter<Fixture> i1(f, 1, 3);
    ConcurrentImporter<Fixture> i2(f, 2, 3);

    i0.fork();
    i1.fork();
    i2.fork();

    i0.wait();
    i1.wait();
    i2.wait();

    auto reader = f.config().create_reader();
    metadata::Collection mdc(*reader, Matcher());
    wassert(actual(mdc.size()) == 60u);

    for (int i = 0; i < 60; ++i)
    {
        auto rt = mdc[i].get<types::reftime::Position>();
        wassert(actual(rt->time.se) == i);
    }
});

this->add_method("read_repack", [](Fixture& f) {
    auto orig_data = f.td.earliest_element().md.getData();

    f.reset_test("step=single");
    f.import_all(f.td);

    auto reader = f.dataset_config()->create_reader();
    reader->query_data(dataset::DataQuery("", true), [&](unique_ptr<Metadata> md) {
        {
            auto checker = f.dataset_config()->create_checker();
            dataset::NullReporter rep;
            try {
                checker->repack(rep, true, dataset::TEST_MISCHIEF_MOVE_DATA);
            } catch (std::exception& e) {
                wassert(actual(e.what()).contains("a read lock is already held"));
            }
        }

        auto data = md->getData();
        wassert(actual(data == orig_data).istrue());
        return false;
    });
});

// Test parallel check and write
this->add_method("write_check", [](Fixture& f) {
    utils::Lock::TestNowait lock_nowait;

    {
        auto writer = f.config().create_writer();
        wassert(actual(writer->acquire(f.td.test_data[0].md)) == dataset::Writer::ACQ_OK);
        writer->flush();

        // Create an error to trigger a call to the reporter that then hangs
        // the checke
        f.makeSegmentedChecker()->test_corrupt_data(f.td.test_data[0].md.sourceBlob().filename, 0);
    }

    CheckForever<Fixture> cf(f);
    cf.start();
    cf.wait_until_ready();

    {
        auto writer = f.makeSegmentedWriter();
        wassert(actual(writer->acquire(f.td.test_data[2].md)) == dataset::Writer::ACQ_OK);

        // Importing on the segment being checked should hang except on dir segments
        if (f.td.format == "odimh5")
            writer->acquire(f.td.test_data[0].md);
        else
            try {
                writer->acquire(f.td.test_data[0].md);
                wassert(actual(0) == 1);
            } catch (std::runtime_error& e) {
                wassert(actual(e.what()).contains("a read lock is already held"));
            }
    }

    cf.kill(9);
    cf.wait();
});


}
}

