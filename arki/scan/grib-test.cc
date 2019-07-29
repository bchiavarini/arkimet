#include "arki/metadata/tests.h"
#include "arki/core/file.h"
#include "arki/scan/grib.h"
#include "arki/types/source.h"
#include "arki/types/origin.h"
#include "arki/types/product.h"
#include "arki/types/level.h"
#include "arki/types/timerange.h"
#include "arki/types/reftime.h"
#include "arki/types/area.h"
#include "arki/types/proddef.h"
#include "arki/types/run.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/metadata/collection.h"
#include "arki/scan/validator.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>

namespace {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_scan_grib");

void is_grib_data(Metadata& md, size_t expected_size)
{
    auto buf = wcallchecked(md.get_data().read());
    wassert(actual(buf.size()) == expected_size);
    wassert(actual(string((const char*)buf.data(), 4)) == "GRIB");
    wassert(actual(string((const char*)buf.data() + expected_size - 4, 4)) == "7777");
}

void Tests::register_tests() {

// Scan a well-known grib file, with no padding between messages
add_method("compact", [] {
    Metadata md;
    metadata::Collection mds;
    scan::Grib scanner;
    vector<uint8_t> buf;
    scanner.test_scan_file("inbound/test.grib1", mds.inserter_func());
    wassert(actual(mds.size()) == 3u);

    // Check the source info
    md = mds[0];
    wassert(actual(md.source().cloneType()).is_source_blob("grib", sys::abspath("."), "inbound/test.grib1", 0, 7218));

    // Check that the source can be read properly
    wassert(is_grib_data(md, 7218));

    // Check notes
    if (md.notes().size() != 1)
    {
        for (size_t i = 0; i < md.notes().size(); ++i)
            cerr << md.notes()[i] << endl;
    }
    wassert(actual(md.notes().size()) == 1u);

    // Check contents
    wassert(actual(md).contains("origin", "GRIB1(200, 0, 101)"));
    wassert(actual(md).contains("product", "GRIB1(200, 140, 229)"));
    wassert(actual(md).contains("level", "GRIB1(1, 0)"));
    wassert(actual(md).contains("timerange", "GRIB1(0, 0s)"));
    wassert(actual(md).contains("area", "GRIB(Ni=97,Nj=73,latfirst=40000000,latlast=46000000,lonfirst=12000000,lonlast=20000000,type=0)"));
    wassert(actual(md).contains("proddef", "GRIB(tod=1)"));
    wassert(actual(md).contains("reftime", "2007-07-08T13:00:00Z"));
    wassert(actual(md).contains("run", "MINUTE(13:00)"));

    // Next grib
    md = mds[1];

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("grib", sys::abspath("."), "inbound/test.grib1", 7218, 34960));

    // Check that the source can be read properly
    wassert(is_grib_data(md, 34960));

    // Check contents
    wassert(actual(md).contains("origin", "GRIB1(80, 255, 100)"));
    wassert(actual(md).contains("product", "GRIB1(80, 2, 2)"));
    wassert(actual(md).contains("level", "GRIB1(102, 0)"));
    wassert(actual(md).contains("timerange", "GRIB1(1)"));
    wassert(actual(md).contains("area", "GRIB(Ni=205,Nj=85,latfirst=30000000,latlast=72000000,lonfirst=-60000000,lonlast=42000000,type=0)"));
    wassert(actual(md).contains("proddef", "GRIB(tod=1)"));
    wassert(actual(md).contains("reftime", "2007-07-07T00:00:00Z"));
    wassert(actual(md).contains("run", "MINUTE(0)"));


    // Last grib
    md = mds[2];

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("grib", sys::abspath("."), "inbound/test.grib1", 42178, 2234));

    // Check that the source can be read properly
    wassert(is_grib_data(md, 2234));

    // Check contents
    wassert(actual(md).contains("origin", "GRIB1(98, 0, 129)"));
    wassert(actual(md).contains("product", "GRIB1(98, 128, 129)"));
    wassert(actual(md).contains("level", "GRIB1(100, 1000)"));
    wassert(actual(md).contains("timerange", "GRIB1(0, 0s)"));
    wassert(actual(md).contains("area", "GRIB(Ni=43,Nj=25,latfirst=55500000,latlast=31500000,lonfirst=-11500000,lonlast=30500000,type=0)"));
    wassert(actual(md).contains("proddef", "GRIB(tod=1)"));
    wassert(actual(md).contains("reftime", "2007-10-09T00:00:00Z"));
    wassert(actual(md).contains("run", "MINUTE(0)"));
});


// Scan a well-known grib file, with extra padding data between messages
add_method("padded", [] {
    Metadata md;
    metadata::Collection mds;
    scan::Grib scanner;
    vector<uint8_t> buf;

    scanner.test_scan_file("inbound/padded.grib1", mds.inserter_func());
    wassert(actual(mds.size()) == 3u);

    md = mds[0];

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("grib", sys::abspath("."), "inbound/padded.grib1", 100, 7218));

    // Check that the source can be read properly
    wassert(is_grib_data(md, 7218));

    // Check contents
    wassert(actual(md).contains("origin", "GRIB1(200, 0, 101)"));
    wassert(actual(md).contains("product", "GRIB1(200, 140, 229)"));
    wassert(actual(md).contains("level", "GRIB1(1, 0)"));
    wassert(actual(md).contains("timerange", "GRIB1(0, 0s)"));
    wassert(actual(md).contains("area", "GRIB(Ni=97,Nj=73,latfirst=40000000,latlast=46000000,lonfirst=12000000,lonlast=20000000,type=0)"));
    wassert(actual(md).contains("proddef", "GRIB(tod=1)"));
    wassert(actual(md).contains("reftime", "2007-07-08T13:00:00Z"));
    wassert(actual(md).contains("run", "MINUTE(13:00)"));

    // Next grib
    md = mds[1];

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("grib", sys::abspath("."), "inbound/padded.grib1", 7418, 34960));

    // Check that the source can be read properly
    wassert(is_grib_data(md, 34960));

    // Check contents
    wassert(actual(md).contains("origin", "GRIB1(80, 255, 100)"));
    wassert(actual(md).contains("product", "GRIB1(80, 2, 2)"));
    wassert(actual(md).contains("level", "GRIB1(102, 0)"));
    wassert(actual(md).contains("timerange", "GRIB1(1)"));
    wassert(actual(md).contains("area", "GRIB(Ni=205,Nj=85,latfirst=30000000,latlast=72000000,lonfirst=-60000000,lonlast=42000000,type=0)"));
    wassert(actual(md).contains("proddef", "GRIB(tod=1)"));
    wassert(actual(md).contains("reftime", "2007-07-07T00:00:00Z"));
    wassert(actual(md).contains("run", "MINUTE(0)"));

    // Last grib
    md = mds[2];

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("grib", sys::abspath("."), "inbound/padded.grib1", 42478, 2234));

    // Check that the source can be read properly
    wassert(is_grib_data(md, 2234));

    // Check contents
    wassert(actual(md).contains("origin", "GRIB1(98, 0, 129)"));
    wassert(actual(md).contains("product", "GRIB1(98, 128, 129)"));
    wassert(actual(md).contains("level", "GRIB1(100, 1000)"));
    wassert(actual(md).contains("timerange", "GRIB1(0, 0s)"));
    wassert(actual(md).contains("area", "GRIB(Ni=43,Nj=25,latfirst=55500000,latlast=31500000,lonfirst=-11500000,lonlast=30500000,type=0)"));
    wassert(actual(md).contains("proddef", "GRIB(tod=1)"));
    wassert(actual(md).contains("reftime", "2007-10-09T00:00:00Z"));
    wassert(actual(md).contains("run", "MINUTE(0)"));
});

add_method("lua_results", [] {
    scan::Grib scanner("", R"(
arki.year = 2008
arki.month = 7
arki.day = 30
arki.hour = 12
arki.minute = 30
arki.second = 0
arki.centre = 98
arki.subcentre = 1
arki.process = 2
arki.origin = 1
arki.table = 1
arki.product = 1
arki.ltype = 1
arki.l1 = 1
arki.l2 = 1
arki.ptype = 1
arki.punit = 1
arki.p1 = 1
arki.p2 = 1
arki.bbox = { { 45.00, 11.00 }, { 46.00, 11.00 }, { 46.00, 12.00 }, { 47.00, 13.00 }, { 45.00, 12.00 } }
)");
    metadata::Collection mds;
    scanner.test_scan_file("inbound/test.grib1", mds.inserter_func());
    wassert(actual(mds.size()) == 3u);

    // Check the source info
    wassert(actual(mds[0].source().cloneType()).is_source_blob("grib", sys::abspath("."), "inbound/test.grib1", 0, 7218));
});

// Test validation
add_method("validation", [] {
    Metadata md;
    vector<uint8_t> buf;

    const scan::Validator& v = scan::grib::validator();

    sys::File fd("inbound/test.grib1", O_RDONLY);
    v.validate_file(fd, 0, 7218);
    v.validate_file(fd, 7218, 34960);
    v.validate_file(fd, 42178, 2234);

    wassert_throws(std::runtime_error, v.validate_file(fd, 1, 7217));
    wassert_throws(std::runtime_error, v.validate_file(fd, 0, 7217));
    wassert_throws(std::runtime_error, v.validate_file(fd, 0, 7219));
    wassert_throws(std::runtime_error, v.validate_file(fd, 7217, 34961));
    wassert_throws(std::runtime_error, v.validate_file(fd, 42178, 2235));
    wassert_throws(std::runtime_error, v.validate_file(fd, 44412, 0));
    wassert_throws(std::runtime_error, v.validate_file(fd, 44412, 10));

    fd.close();

    metadata::TestCollection mdc("inbound/test.grib1");
    buf = mdc[0].get_data().read();

    wassert(v.validate_buf(buf.data(), buf.size()));
    wassert_throws(std::runtime_error, v.validate_buf((const char*)buf.data()+1, buf.size()-1));
    wassert_throws(std::runtime_error, v.validate_buf(buf.data(), buf.size()-1));
});

// Test scanning layers instead of levels
add_method("layers", [] {
    Metadata md;
    metadata::Collection mds;
    scan::Grib scanner;
    vector<uint8_t> buf;
    scanner.test_scan_file("inbound/layer.grib1", mds.inserter_func());
    wassert(actual(mds.size()) == 1u);

    md = mds[0];

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("grib", sys::abspath("."), "inbound/layer.grib1", 0, 30682));

    // Check that the source can be read properly
    wassert(is_grib_data(md, 30682));

    // Check contents
    wassert(actual(md).contains("origin", "GRIB1(200, 255, 45)"));
    wassert(actual(md).contains("product", "GRIB1(200, 2, 33)"));
    wassert(actual(md).contains("level", "GRIB1(110, 1, 2)"));
    wassert(actual(md).contains("timerange", "Timedef(0s, 254, 0s)"));
    wassert(actual(md).contains("area", "GRIB(Ni=169,Nj=181,latfirst=-21125000,latlast=-9875000,lonfirst=-2937000,lonlast=7563000,latp=-32500000,lonp=10000000,rot=0,type=10)"));
    wassert(actual(md).contains("proddef", "GRIB(tod=0)"));
    wassert(actual(md).contains("reftime", "2009-09-02T00:00:00Z"));
    wassert(actual(md).contains("run", "MINUTE(0)"));
});

// Scan a know item for which grib_api changed behaviour
add_method("proselvo", [] {
    Metadata md;
    scan::Grib scanner;
    vector<uint8_t> buf;
    metadata::Collection mds;
    scanner.test_scan_file("inbound/proselvo.grib1", mds.inserter_func());
    wassert(actual(mds.size()) == 1u);

    md = mds[0];

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("grib", sys::abspath("."), "inbound/proselvo.grib1", 0, 298));

    // Check that the source can be read properly
    wassert(is_grib_data(md, 298));

    // Check contents
    wassert(actual(md).contains("origin", "GRIB1(80, 98, 131)"));
    wassert(actual(md).contains("product", "GRIB1(80, 2, 79)"));
    wassert(actual(md).contains("level", "GRIB1(1)"));
    wassert(actual(md).contains("timerange", "GRIB1(0, 0s)"));
    wassert(actual(md).contains("area", "GRIB(Ni=231,Nj=265,latfirst=-16125000,latlast=375000,lonfirst=-5125000,lonlast=9250000,latp=-40000000,lonp=10000000,rot=0,type=10)"));
    wassert(actual(md).contains("proddef", "GRIB(ld=1,mt=9,nn=0,tod=1)"));
    wassert(actual(md).contains("reftime", "2010-08-11T12:00:00Z"));
    wassert(actual(md).contains("run", "MINUTE(12:00)"));
});

// Scan a know item for which grib_api changed behaviour
add_method("cleps", [] {
    Metadata md;
    scan::Grib scanner;
    vector<uint8_t> buf;
    metadata::Collection mds;
    scanner.test_scan_file("inbound/cleps_pf16_HighPriority.grib2", mds.inserter_func());
    wassert(actual(mds.size()) == 1u);

    md = mds[0];

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("grib", sys::abspath("."), "inbound/cleps_pf16_HighPriority.grib2", 0, 432));

    // Check that the source can be read properly
    wassert(is_grib_data(md, 432));

    wassert(actual(md).contains("origin", "GRIB2(250, 98, 4, 255, 131)"));
    wassert(actual(md).contains("product", "GRIB2(250, 2, 0, 0, 5, 0)"));
    wassert(actual(md).contains("level", "GRIB2S(1, -, -)"));
    wassert(actual(md).contains("timerange", "Timedef(0, 254, 0s)"));
    wassert(actual(md).contains("area", "GRIB(Ni=511,Nj=415,latfirst=-16125000,latlast=9750000,lonfirst=344250000,lonlast=16125000,latp=-40000000,lonp=10000000,rot=0,tn=1)"));
    wassert(actual(md).contains("proddef", "GRIB(mc=ti,mt=0,pf=16,tf=16,tod=4,ty=3)"));
    wassert(actual(md).contains("reftime", "2013-10-22T00:00:00"));
    wassert(actual(md).contains("run", "MINUTE(00:00)"));
});

// Scan a GRIB2 with experimental UTM areas
add_method("utm_areas", [] {
#ifndef ARPAE_TESTS
    throw TestSkipped("ARPAE GRIB support not available");
#endif
    Metadata md;
    scan::Grib scanner;
    metadata::Collection mds;
    scanner.test_scan_file("inbound/calmety_20110215.grib2", mds.inserter_func());
    wassert(actual(mds.size()) == 1u);
    md = mds[0];

    wassert(actual(md).contains("origin", "GRIB2(00200, 00000, 000, 000, 203)"));
    wassert(actual(md).contains("product", "GRIB2(200, 0, 200, 33, 5, 0)"));
    wassert(actual(md).contains("level", "GRIB2S(103, 0, 10)"));
    wassert(actual(md).contains("timerange", "Timedef(0s, 254, 0s)"));
    wassert(actual(md).contains("area", "GRIB(Ni=90, Nj=52, fe=0, fn=0, latfirst=4852500, latlast=5107500, lonfirst=402500, lonlast=847500, tn=32768, utm=1, zone=32)"));
    wassert(actual(md).contains("proddef", "GRIB(tod=0)"));
    wassert(actual(md).contains("reftime", "2011-02-15T00:00:00Z"));
    wassert(actual(md).contains("run", "MINUTE(0)"));
});

// Check scanning of some Timedef cases
add_method("ninfa", [] {
#ifndef ARPAE_TESTS
    throw TestSkipped("ARPAE GRIB support not available");
#endif
    {
        scan::Grib scanner;
        metadata::Collection mds;
        scanner.test_scan_file("inbound/ninfa_ana.grib2", mds.inserter_func());
        wassert(actual(mds.size()) == 1u);
        wassert(actual(mds[0]).contains("timerange", "Timedef(0s,254,0s)"));
    }
    {
        scan::Grib scanner;
        metadata::Collection mds;
        scanner.test_scan_file("inbound/ninfa_forc.grib2", mds.inserter_func());
        wassert(actual(mds.size()) == 1u);
        wassert(actual(mds[0]).contains("timerange", "Timedef(3h,254,0s)"));
    }
});

// Check scanning COSMO nudging timeranges
add_method("cosmo_nudging", [] {
#ifndef ARPAE_TESTS
    throw TestSkipped("ARPAE GRIB support not available");
#endif
    ARKI_UTILS_TEST_INFO(info);

    {
        metadata::TestCollection mdc("inbound/cosmonudging-t2.grib1");
        wassert(actual(mdc.size()) == 35u);
        for (unsigned i = 0; i < 5; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s,254,0s)"));
        wassert(actual(mdc[5]).contains("timerange", "Timedef(0s, 2, 1h)"));
        wassert(actual(mdc[6]).contains("timerange", "Timedef(0s, 3, 1h)"));
        for (unsigned i = 7; i < 13; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s,254,0s)"));
        wassert(actual(mdc[13]).contains("timerange", "Timedef(0s, 1, 12h)"));
        for (unsigned i = 14; i < 19; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s,254,0s)"));
        wassert(actual(mdc[19]).contains("timerange", "Timedef(0s, 1, 12h)"));
        wassert(actual(mdc[20]).contains("timerange", "Timedef(0s, 1, 12h)"));
        for (unsigned i = 21; i < 26; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s,254,0s)"));
        wassert(actual(mdc[26]).contains("timerange", "Timedef(0s, 1, 12h)"));
        for (unsigned i = 27; i < 35; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s, 0, 12h)"));
    }
    {
        metadata::TestCollection mdc("inbound/cosmonudging-t201.grib1");
        wassert(actual(mdc.size()) == 33u);
        wassert(actual(mdc[0]).contains("timerange", "Timedef(0s, 0, 12h)"));
        wassert(actual(mdc[1]).contains("timerange", "Timedef(0s, 0, 12h)"));
        wassert(actual(mdc[2]).contains("timerange", "Timedef(0s, 0, 12h)"));
        for (unsigned i = 3; i < 16; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s,254,0s)"));
        wassert(actual(mdc[16]).contains("timerange", "Timedef(0s, 1, 12h)"));
        wassert(actual(mdc[17]).contains("timerange", "Timedef(0s, 1, 12h)"));
        for (unsigned i = 18; i < 26; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s,254,0s)"));
        wassert(actual(mdc[26]).contains("timerange", "Timedef(0s, 2, 1h)"));
        for (unsigned i = 27; i < 33; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s,254,0s)"));
    }
    {
        metadata::TestCollection mdc("inbound/cosmonudging-t202.grib1");
        wassert(actual(mdc.size()) == 11u);
        for (unsigned i = 0; i < 11; ++i)
            wassert(actual(mdc[i]).contains("timerange", "Timedef(0s,254,0s)"));
    }


    // Shortcut to read one single GRIB from a file
    struct OneGrib
    {
        arki::utils::tests::LocationInfo& arki_utils_test_location_info;
        Metadata md;

        OneGrib(arki::utils::tests::LocationInfo& info) : arki_utils_test_location_info(info) {}
        void read(const char* fname)
        {
            arki_utils_test_location_info() << "Sample: " << fname;
            metadata::TestCollection mdc(fname);
            wassert(actual(mdc.size()) == 1u);
            md = wcallchecked(mdc[0]);
        }
    };

    OneGrib md(info);

    md.read("inbound/cosmonudging-t203.grib1");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,254,0s)"));

    md.read("inbound/cosmo/anist_1.grib");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,254,0s)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=0)"));

    md.read("inbound/cosmo/anist_1.grib2");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,254,0s)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=0)"));

    md.read("inbound/cosmo/fc0ist_1.grib");
    wassert(actual(md.md.get<Timerange>()->to_timedef()) == std::string("Timedef(0s,254,0s)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=1)"));

    md.read("inbound/cosmo/anproc_1.grib");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,1,1h)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=0)"));

    md.read("inbound/cosmo/anproc_2.grib");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,0,1h)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=0)"));

    md.read("inbound/cosmo/anproc_3.grib");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,2,1h)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=0)"));

    md.read("inbound/cosmo/anproc_4.grib");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,0,12h)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=0)"));

    md.read("inbound/cosmo/anproc_1.grib2");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,0,24h)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=0)"));

    md.read("inbound/cosmo/anproc_2.grib2");
    wassert(actual(md.md).contains("timerange", "Timedef(0s,1,24h)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=0)"));

    md.read("inbound/cosmo/fcist_1.grib");
    wassert(actual(md.md.get<Timerange>()->to_timedef()) == string("Timedef(6h,254,0s)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=1)"));

    md.read("inbound/cosmo/fcist_1.grib2");
    wassert(actual(md.md).contains("timerange", "Timedef(6h,254,0s)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=1)"));

    md.read("inbound/cosmo/fcproc_1.grib");
    wassert(actual(md.md.get<Timerange>()->to_timedef()) == string("Timedef(6h,1,6h)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=1)"));

    md.read("inbound/cosmo/fcproc_3.grib2");
    wassert(actual(md.md).contains("timerange", "Timedef(48h,1,24h)"));
    wassert(actual(md.md).contains("proddef", "GRIB(tod=1)"));
});

#if 0
// Check opening very long GRIB files for scanning
// TODO: needs skipping of there are no holes
// FIXME: cannot reenable it because currently there is no interface for
// opening without scanning, and eccodes takes a long time to skip all those
// null bytes
add_method("bigfile", [] {
    scan::Grib scanner;
    int fd = open("bigfile.grib1", O_WRONLY | O_CREAT, 0644);
    ensure(fd != -1);
    ensure(ftruncate(fd, 0xFFFFFFFF) != -1);
    close(fd);
    wassert(scanner.test_open("bigfile.grib1"));
});
#endif

add_method("issue120", [] {
    Metadata md;
    scan::Grib scanner;
    metadata::Collection mds;
    scanner.test_scan_file("inbound/oddunits.grib", mds.inserter_func());
    wassert(actual(mds.size()) == 1u);
    md = mds[0];

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("grib", sys::abspath("."), "inbound/oddunits.grib", 0, 245));

    // Check that the source can be read properly
    wassert(is_grib_data(md, 245));

    wassert(actual(md).contains("origin", "GRIB2(00080, 00255, 005, 255, 015)"));
    wassert(actual(md).contains("product", "GRIB2(00080, 000, 001, 052, 011, 001)"));
    wassert(actual(md).contains("level", "GRIB2S(1, -, -)"));
    wassert(actual(md).contains("timerange", "Timedef(27h, 4, 24h)"));
    wassert(actual(md).contains("area", "GRIB(Ni=576, Nj=701, latfirst=-8500000, latlast=5500000, latp=-47000000, lonfirst=-3800000, lonlast=7700000, lonp=10000000, rot=0, tn=1)"));
    wassert(actual(md).contains("proddef", "GRIB(tod=5)"));
    wassert(actual(md).contains("reftime", "2018-01-25T21:00:00"));
    wassert(actual(md).contains("run", "MINUTE(21:00)"));
});

}

}
