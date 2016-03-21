#include <arki/tests/tests.h>
#include <arki/dataset/gridquery.h>
#include <arki/dataset/memory.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/matcher.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/scan/grib.h>
#include <arki/dispatcher.h>
#include <arki/runtime/config.h>
#include <arki/utils/files.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::tests;
using arki::core::Time;

struct arki_dataset_gridquery_shar {
    ConfigFile config;
    dataset::Reader* ds;

	arki_dataset_gridquery_shar() : ds(0)
	{
		// Cleanup the test datasets
		system("rm -rf testds ; mkdir testds");
		system("rm -rf error ; mkdir error");

        // In-memory dataset configuration
        string conf =
            "[testds]\n"
            "type = ondisk2\n"
            "step = daily\n"
            "filter = origin: GRIB1\n"
            "index = origin, reftime\n"
            "name = testds\n"
            "path = testds\n"
            "\n"
            "[error]\n"
            "type = error\n"
            "step = daily\n"
            "name = error\n"
            "path = error\n";
        config.parse(conf, "(memory)");

        // Import data into the datasets
        Metadata md;
        metadata::Collection mdc;
        scan::Grib scanner;
        RealDispatcher dispatcher(config);
        scanner.open("inbound/test.grib1");
        ensure(scanner.next(md));
        ensure_equals(dispatcher.dispatch(unique_ptr<Metadata>(new Metadata(md)), mdc.inserter_func()), Dispatcher::DISP_OK);
        ensure(scanner.next(md));
        ensure_equals(dispatcher.dispatch(unique_ptr<Metadata>(new Metadata(md)), mdc.inserter_func()), Dispatcher::DISP_OK);
        ensure(scanner.next(md));
        ensure_equals(dispatcher.dispatch(unique_ptr<Metadata>(new Metadata(md)), mdc.inserter_func()), Dispatcher::DISP_OK);
        ensure(!scanner.next(md));
        dispatcher.flush();

        ds = dataset::Reader::create(*config.section("testds"));
    }

	~arki_dataset_gridquery_shar()
	{
		if (ds) delete ds;
	}
};
TESTGRP(arki_dataset_gridquery);


// Test GridQuery
def_test(1)
{
	dataset::GridQuery gq(*ds);

    Time t(2007, 7, 8, 13, 0, 0);

	// Trivially query only one item
	gq.add(Matcher::parse("origin:GRIB1,200,0,101;product:GRIB1,200,140,229"));
	gq.addTime(t);
	gq.consolidate();

	ensure_equals(gq.expectedItems(), 1u);

	ItemSet is;
	is.set(types::origin::GRIB1::create(200, 0, 101));
	is.set(types::product::GRIB1::create(200, 140, 229));
	is.set(types::reftime::Position::create(t));

	ensure(not gq.satisfied());
	ensure(gq.checkAndMark(is));
	ensure(not gq.checkAndMark(is));
	ensure(gq.satisfied());
}

// Test adding an entry which does not expand to anything
def_test(2)
{
	runtime::readMatcherAliasDatabase();

    // Build a test dataset
    string md_yaml(
        "Source: BLOB(grib1,/dev/null:0+186196)\n"
        "Origin: GRIB1(200, 255, 047)\n"
        "Product: GRIB1(200, 002, 061)\n"
        "Level: GRIB1(001)\n"
        "Timerange: GRIB1(004, 000h, 012h)\n"
        "Reftime: 2010-05-03T00:00:00Z\n"
        "Area: GRIB(Ni=297, Nj=313, latfirst=-25000000, latlast=-5500000, latp=-32500000, lonfirst=-8500000, lonlast=10000000, lonp=10000000, rot=0, type=10)\n"
        "Run: MINUTE(00:00)\n"
    );
    auto reader = LineReader::from_chars(md_yaml.data(), md_yaml.size());
    unique_ptr<Metadata> md = Metadata::create_empty();
    md->readYaml(*reader, "(test memory buffer)");

    dataset::Memory ds;
    ds.acquire(move(md));

    // Build the grid query
    dataset::GridQuery gq(ds);

    try {
        gq.add(Matcher::parse("timerange:c012; level:g00; product:u"));
        ensure(false);
    } catch (std::exception& c) {
        ensure(string(c.what()).find("no data which correspond to the matcher") != string::npos);
    }
    gq.add(Matcher::parse("timerange:c012; level:g00; product:tp"));

    Time t(2010, 05, 03, 0, 0, 0);
    gq.addTime(t);

    gq.consolidate();

    ensure_equals(gq.expectedItems(), 1u);
}

}
