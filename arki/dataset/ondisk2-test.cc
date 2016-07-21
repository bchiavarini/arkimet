#include <arki/dataset/tests.h>
#include <arki/exceptions.h>
#include <arki/dataset/ondisk2/reader.h>
#include <arki/dataset/ondisk2/writer.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/types/source/blob.h>
#include <arki/summary.h>
#include <arki/matcher.h>
#include <arki/scan/grib.h>
#include <arki/scan/any.h>
#include <arki/utils/files.h>
#include <arki/utils/sys.h>
#include <arki/wibble/sys/childprocess.h>
#include <unistd.h>
#include <sys/fcntl.h>
#include <sstream>
#include <iostream>

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;
using namespace arki::types;

namespace {

struct Fixture : public DatasetTest {
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            type=ondisk2
            step=daily
        )");
    }

    // Recreate the dataset importing data into it
    void clean_and_import(const std::string& testfile="inbound/test.grib1")
    {
        DatasetTest::clean_and_import(testfile);
        wassert(ensure_localds_clean(3, 3));
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
};

Tests tests("arki_dataset_ondisk2");


void Tests::register_tests() {

// Test acquiring data
add_method("acquire", [](Fixture& f) {
    metadata::Collection mdc("inbound/test.grib1");

    auto writer = f.makeOndisk2Writer();

    // Import once in the empty dataset
    wassert(actual(writer->acquire(mdc[0])) == dataset::Writer::ACQ_OK);

    // See if we catch duplicate imports
    wassert(actual(writer->acquire(mdc[0])) == dataset::Writer::ACQ_ERROR_DUPLICATE);

    // Flush the changes and check that everything is allright
    writer->flush();

    wassert(actual_file("testds/2007/07-08.grib").exists());
    wassert(actual_file("testds/index.sqlite").exists());
    wassert(actual(sys::timestamp("testds/2007/07-08.grib")) <= sys::timestamp("testds/index.sqlite"));
});

// Test replacing an element
add_method("replace", [](Fixture& f) {
    f.import();

    metadata::Collection mdc = f.query(Matcher::parse("origin:GRIB1,80"));
    wassert(actual(mdc.size()) == 1u);

    // Take note of the original source
    unique_ptr<source::Blob> blob1(mdc[0].sourceBlob().clone());

    // Reimport, replacing
    {
        auto writer = f.makeOndisk2Writer();
        if (writer->acquire(mdc[0], dataset::Writer::REPLACE_ALWAYS) != dataset::Writer::ACQ_OK)
        {
            std::vector<Note> notes = mdc[0].notes();
            for (size_t i = 0; i < notes.size(); ++i)
                cerr << " md note: " << notes[i] << endl;
            wassert(throw TestFailed("acquire result was not ACQ_OK"));
        }
        writer->flush();
    }

    // Fetch the element again
    mdc = f.query(Matcher::parse("origin:GRIB1,80"));
    wassert(actual(mdc.size()) == 1u);

    // Get the new source
    unique_ptr<source::Blob> blob2(mdc[0].sourceBlob().clone());

    // Ensure that it's on the same file (since the reftime did not change)
    wassert(actual(blob1->filename) == blob2->filename);

    // Ensure that it's on a different position
    wassert(actual(blob1->offset) < blob2->offset);

    // Ensure that it's the same length
    wassert(actual(blob1->size) == blob2->size);
});

// Test removing an element
add_method("remove", [](Fixture& f) {
    f.import();

    metadata::Collection mdc = f.query(Matcher::parse("origin:GRIB1,200"));

    // Check that it has a source and metadata element
    wassert(actual(mdc[0].has_source_blob()).istrue());

    // Remove it
    {
        auto writer = f.makeOndisk2Writer();
        writer->remove(mdc[0]);
        writer->flush();
    }

    // Check that it does not have a source and metadata element
    wassert(actual(mdc[0].has_source()).isfalse());

    // Try to fetch the element again
    mdc = f.query(Matcher::parse("origin:GRIB1,200"));
    wassert(actual(mdc.size()) == 0u);
});


#if 0
struct arki_dataset_ondisk2_shar {
	ConfigFile config;
	ConfigFile configAll;
    testdata::GRIBData tdata_grib;
    testdata::BUFRData tdata_bufr;
    testdata::VM2Data tdata_vm2;
    testdata::ODIMData tdata_odim;

	arki_dataset_ondisk2_shar()
	{
		// Cleanup the test datasets
		system("rm -rf testall/*");
		system("rm -rf test200/*");
		system("rm -rf test80/*");
		system("rm -rf test98/*");

		// In-memory dataset configuration
		string conf =
			"[test200]\n"
			"type = ondisk2\n"
			"step = daily\n"
			"filter = origin: GRIB1,200\n"
			"index = origin, reftime\n"
			"unique = reftime, origin, product, level, timerange, area\n"
			"name = test200\n"
			"path = test200\n"
			"\n"
			"[test80]\n"
			"type = ondisk2\n"
			"step = daily\n"
			"filter = origin: GRIB1,80\n"
			"index = origin, reftime\n"
			"unique = reftime, origin, product, level, timerange, area\n"
			"name = test80\n"
			"path = test80\n"
			"\n"
			"[test98]\n"
			"type = ondisk2\n"
			"step = daily\n"
			"filter = origin: GRIB1,98\n"
			"unique = reftime, origin, product, level, timerange, area\n"
			"name = test98\n"
			"path = test98\n";
        config.parse(conf);
        config.section("test200")->setValue("path", sys::abspath("test200"));
        config.section("test80")->setValue("path", sys::abspath("test80"));
        config.section("test98")->setValue("path", sys::abspath("test98"));


        // In-memory dataset configuration
        string conf1 =
            "[testall]\n"
            "type = ondisk2\n"
            "step = daily\n"
            "filter = origin: GRIB1\n"
            "index = origin, reftime\n"
            "unique = reftime, origin, product, level, timerange, area\n"
            "name = testall\n"
            "path = testall\n";
        configAll.parse(conf1, "(memory)");
    }

	void acquireSamples()
	{
		// Cleanup the test datasets
		system("rm -rf test200/*");
		system("rm -rf test80/*");
		system("rm -rf test98/*");

		// Import data into the datasets
		Metadata md;
		scan::Grib scanner;
		dataset::ondisk2::Writer d200(*config.section("test200"));
		dataset::ondisk2::Writer d80(*config.section("test80"));
		dataset::ondisk2::Writer d98(*config.section("test98"));
        scanner.open("inbound/test.grib1");
        ensure(scanner.next(md));
        ensure_equals(d200.acquire(md), Writer::ACQ_OK);
        ensure(scanner.next(md));
        ensure_equals(d80.acquire(md), Writer::ACQ_OK);
        ensure(scanner.next(md));
        ensure_equals(d98.acquire(md), Writer::ACQ_OK);
        ensure(!scanner.next(md));

		// Finalise
		d200.flush();
		d80.flush();
		d98.flush();
	}

	void acquireSamplesAllInOne()
	{
		// Cleanup the test datasets
		system("rm -rf testall/*");

        // Import data into the datasets
        Metadata md;
        scan::Grib scanner;
        dataset::ondisk2::Writer all(*configAll.section("testall"));
        scanner.open("inbound/test.grib1");
        ensure(scanner.next(md));
        ensure_equals(all.acquire(md), Writer::ACQ_OK);
        ensure(scanner.next(md));
        ensure_equals(all.acquire(md), Writer::ACQ_OK);
        ensure(scanner.next(md));
        ensure_equals(all.acquire(md), Writer::ACQ_OK);
        ensure(!scanner.next(md));

        // Finalise
        all.flush();
    }
};
TESTGRP(arki_dataset_ondisk2);

// Work-around for a bug in old gcc versions
#if __GNUC__ && __GNUC__ < 4
static string emptystring;
#define string() emptystring
#endif


// Test reindexing
def_test(7)
{
	// TODO
#if 0
	acquireSamples();

	// Try a query
	{
        ondisk2::Reader reader(*config.section("test200"));
		ensure(reader.hasWorkingIndex());
		metadata::Collection mdc;
		reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200")), mdc);
		ensure_equals(mdc.size(), 1u);
	}

	// Rebuild the index
	{
		dataset::ondisk2::Writer d200(*config.section("test200"));
		// Run a full maintenance run
		d200.invalidateAll();
		metadata::Collection mdc;
		stringstream log;
		FullMaintenance fr(log, mdc);
		//FullMaintenance fr(cerr, mdc);
		d200.maintenance(fr);
	}

	// See that the query still works
	{
        ondisk2::Reader reader(*config.section("test200"));
		ensure(reader.hasWorkingIndex());
		metadata::Collection mdc;
		reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200")), mdc);
		ensure_equals(mdc.size(), 1u);
	}
#endif
}

// Test querying the summary
def_test(8)
{
    acquireSamples();
    ondisk2::Reader reader(*config.section("test200"));
    Summary summary;
    reader.query_summary(Matcher::parse("origin:GRIB1,200"), summary);
    ensure_equals(summary.count(), 1u);
}

// Test querying the summary by reftime
def_test(9)
{
    acquireSamples();
    ondisk2::Reader reader(*config.section("test200"));
    Summary summary;
    //system("bash");
    reader.query_summary(Matcher::parse("reftime:>=2007-07"), summary);
    ensure_equals(summary.count(), 1u);
}

// Test acquiring data with replace=1
def_test(10)
{
	// Clean the dataset
	system("rm -rf test200/*");

	Metadata md;

	// Import once
	{
		scan::Grib scanner;
		scanner.open("inbound/test.grib1");

        dataset::ondisk2::Writer d200(*config.section("test200"));
        size_t count;
        for (count = 0; scanner.next(md); ++count)
            ensure(d200.acquire(md) == Writer::ACQ_OK);
        ensure_equals(count, 3u);
        d200.flush();
    }

	// Import again, make sure they're all duplicates
	{
		scan::Grib scanner;
		scanner.open("inbound/test.grib1");

        dataset::ondisk2::Writer d200(*config.section("test200"));
        size_t count;
        for (count = 0; scanner.next(md); ++count)
            ensure(d200.acquire(md) == Writer::ACQ_ERROR_DUPLICATE);
        ensure_equals(count, 3u);
        d200.flush();
    }

	// Import again with replace=true, make sure they're all ok
	{
		scan::Grib scanner;
		scanner.open("inbound/test.grib1");

        ConfigFile cfg = *config.section("test200");
        cfg.setValue("replace", "true");
        dataset::ondisk2::Writer d200(cfg);
        size_t count;
        for (count = 0; scanner.next(md); ++count)
            ensure(d200.acquire(md) == Writer::ACQ_OK);
        ensure_equals(count, 3u);
        d200.flush();
    }

	// Make sure all the data files need repack, as they now have got deleted data inside
	//ensure(hasPackFlagfile("test200/2007/07-08.grib"));
	//ensure(hasPackFlagfile("test200/2007/07-07.grib"));
	//ensure(hasPackFlagfile("test200/2007/10-09.grib"));

    // Test querying the dataset
    {
        ondisk2::Reader reader(*config.section("test200"));
        metadata::Collection mdc(reader, Matcher());
        ensure_equals(mdc.size(), 3u);

        // Make sure we're not getting the deleted element
        const source::Blob& blob0 = mdc[0].sourceBlob();
        ensure(blob0.offset > 0);
        const source::Blob& blob1 = mdc[1].sourceBlob();
        ensure(blob1.offset > 0);
        const source::Blob& blob2 = mdc[2].sourceBlob();
        ensure(blob2.offset > 0);
    }

    // Test querying the summary
    {
        ondisk2::Reader reader(*config.section("test200"));
        Summary summary;
        reader.query_summary(Matcher(), summary);
        ensure_equals(summary.count(), 3u);
    }
}

// Test querying the first reftime extreme of the summary
def_test(11)
{
    acquireSamplesAllInOne();
    ondisk2::Reader reader(*configAll.section("testall"));
    Summary summary;
    reader.query_summary(Matcher(), summary);
    ensure_equals(summary.count(), 3u);

    unique_ptr<Reftime> rt = summary.getReferenceTime();
    ensure_equals(rt->style(), Reftime::PERIOD);
    unique_ptr<reftime::Period> p = downcast<reftime::Period>(move(rt));
    metadata::Collection mdc(reader, Matcher::parse("origin:GRIB1,80; reftime:=" + p->begin.to_iso8601()));
    ensure_equals(mdc.size(), 1u);

    mdc.clear();
    mdc.add(reader, Matcher::parse("origin:GRIB1,98; reftime:=" + p->end.to_iso8601()));
    ensure_equals(mdc.size(), 1u);
}

// Test querying data using index
def_test(12)
{
    acquireSamplesAllInOne();
    ondisk2::Reader reader(*configAll.section("testall"));
    sys::File out(sys::File::mkstemp("test"));
    dataset::ByteQuery bq;
    bq.setData(Matcher::parse("reftime:=2007"));
    reader.query_bytes(bq, out);
    string res = sys::read_file(out.name());
    ensure_equals(res.size(), 44412u);
}

// Test acquiring data on a compressed file
def_test(14)
{
	// In-memory dataset configuration
	string conf =
		"[testall]\n"
		"type = ondisk2\n"
		"step = yearly\n"
		"filter = origin: GRIB1\n"
		"index = origin, reftime\n"
		"unique = reftime, origin, product, level, timerange, area\n"
		"name = testall\n"
		"path = testall\n";
    config.parse(conf, "(memory)");

	// Clean the dataset
	system("rm -rf testall/*");

	Metadata md;
	scan::Grib scanner;
	scanner.open("inbound/test.grib1");

    // Import the first two
    {
        dataset::ondisk2::Writer all(*config.section("testall"));
        ensure(scanner.next(md));
        ensure_equals(all.acquire(md), Writer::ACQ_OK);
    }
    ensure(sys::exists("testall/20/2007.grib"));

    // Compress what is imported so far
    {
        ondisk2::Reader reader(*config.section("testall"));
        metadata::Collection mdc(reader, Matcher::parse(""));
        ensure_equals(mdc.size(), 1u);
        mdc.compressDataFile(1024, "metadata file testall/20/2007.grib");
        sys::unlink_ifexists("testall/20/2007.grib");
    }
    ensure(!sys::exists("testall/20/2007.grib"));

    // Import the last data
    {
        dataset::ondisk2::Writer all(*config.section("testall"));
        ensure(scanner.next(md));
        try {
            ensure_equals(all.acquire(md), Writer::ACQ_OK);
        } catch (std::exception& e) {
            ensure(string(e.what()).find("cannot update compressed data files") != string::npos);
        }
    }
    ensure(!sys::exists("testall/20/2007.grib"));
}

// Test Update Sequence Number replacement strategy
def_test(15)
{
    // Configure a BUFR dataset
    system("rm -rf testbufr/*");
    string conf =
        "[testbufr]\n"
        "type = ondisk2\n"
        "step = daily\n"
        "filter = origin:BUFR\n"
        "unique = reftime\n"
        "name = testbufr\n"
        "path = testbufr\n";
    ConfigFile cfg;
    cfg.parse(conf, "(memory)");

    dataset::ondisk2::Writer bd(*cfg.section("testbufr"));

    metadata::Collection mdc("inbound/synop-gts.bufr");
    metadata::Collection mdc_upd("inbound/synop-gts-usn2.bufr");

    wassert(actual(mdc.size()) == 1);

    // Acquire
    ensure_equals(bd.acquire(mdc[0]), Writer::ACQ_OK);

    // Acquire again: it fails
    ensure_equals(bd.acquire(mdc[0]), Writer::ACQ_ERROR_DUPLICATE);

    // Acquire again: it fails even with a higher USN
    ensure_equals(bd.acquire(mdc_upd[0]), Writer::ACQ_ERROR_DUPLICATE);

    // Acquire with replace: it works
    ensure_equals(bd.acquire(mdc[0], Writer::REPLACE_ALWAYS), Writer::ACQ_OK);

    // Acquire with USN: it works, since USNs the same as the existing ones do overwrite
    ensure_equals(bd.acquire(mdc[0], Writer::REPLACE_HIGHER_USN), Writer::ACQ_OK);

    // Acquire with a newer USN: it works
    ensure_equals(bd.acquire(mdc_upd[0], Writer::REPLACE_HIGHER_USN), Writer::ACQ_OK);

    // Acquire with the lower USN: it fails
    ensure_equals(bd.acquire(mdc[0], Writer::REPLACE_HIGHER_USN), Writer::ACQ_ERROR_DUPLICATE);

    // Acquire with the same high USN: it works, since USNs the same as the existing ones do overwrite
    ensure_equals(bd.acquire(mdc_upd[0], Writer::REPLACE_HIGHER_USN), Writer::ACQ_OK);

    // Try to query the element and see if it is the right one
    {
        unique_ptr<Reader> testds(Reader::create(*cfg.section("testbufr")));
        metadata::Collection mdc_read(*testds, Matcher::parse("origin:BUFR"));
        ensure_equals(mdc_read.size(), 1u);
        int usn;
        ensure_equals(scan::update_sequence_number(mdc_read[0], usn), true);
        ensure_equals(usn, 2);
    }
}

// Test a dataset with very large mock files in it
def_test(16)
{
    // A dataset with hole files
    string conf =
        "type = ondisk2\n"
        "step = daily\n"
        "unique = reftime\n"
        "segments = dir\n"
        "mockdata = true\n"
        "name = testholes\n"
        "path = testholes\n";
    {
        unique_ptr<dataset::LocalWriter> writer(make_dataset_writer(conf));

        // Import 24*30*10Mb=7.2Gb of data
        for (unsigned day = 1; day <= 30; ++day)
        {
            for (unsigned hour = 0; hour < 24; ++hour)
            {
                Metadata md = testdata::make_large_mock("grib", 10*1024*1024, 12, day, hour);
                Writer::AcquireResult res = writer->acquire(md);
                wassert(actual(res) == Writer::ACQ_OK);
            }
        }
        writer->flush();
    }

    unique_ptr<dataset::LocalChecker> writer(make_dataset_checker(conf));
    wassert(actual(writer.get()).check_clean());

    // Query it, without data
    std::unique_ptr<Reader> reader(make_dataset_reader(conf));
    metadata::Collection mdc;
    mdc.add(*reader, Matcher::parse(""));
    wassert(actual(mdc.size()) == 720u);

    // Query it, streaming its data to /dev/null
    sys::File out("/dev/null", O_WRONLY);
    dataset::ByteQuery bq;
    bq.setData(Matcher());
    reader->query_bytes(bq, out);
}

#endif

}

}