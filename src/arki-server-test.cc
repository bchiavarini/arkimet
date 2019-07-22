#include <arki/types/tests.h>
#include <arki/dataset.h>
#include <arki/dataset/http.h>
#include <arki/metadata.h>
#include <arki/metadata/data.h>
#include <arki/metadata/collection.h>
#include <arki/matcher.h>
#include <arki/summary.h>
#include <arki/scan/grib.h>
#include <arki/dispatcher.h>
#include <arki/types/source/blob.h>
#include <arki/utils.h>
#include <arki/utils/sys.h>
#include <arki/binary.h>
#include <arki/runtime/processor.h>
#include <sstream>
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
} test("arki_server");

void Tests::register_tests() {

// Query the configuration
add_method("config", [] {
    auto config = dataset::http::Reader::load_cfg_sections("http://localhost:7117");

    wassert(actual(config.section("test200")).istrue());
    wassert(actual(config.section("test200")->value("server")) == "http://localhost:7117");

    wassert(actual(config.section("test80")).istrue());
    wassert(actual(config.section("test80")->value("server")) == "http://localhost:7117");

    wassert(actual(config.section("error")).istrue());
    wassert(actual(config.section("error")->value("server")) == "http://localhost:7117");
});

// Test querying the datasets, metadata only
add_method("metadata", [] {
    auto config = dataset::http::Reader::load_cfg_sections("http://localhost:7117");

    unique_ptr<dataset::Reader> testds(dataset::Reader::create(*config.section("test200")));
    metadata::Collection mdc(*testds, Matcher::parse("origin:GRIB1,200"));
    wassert(actual(mdc.size()) == 1u);

    // Check that the source record that comes out is ok
    wassert(actual_type(mdc[0].source()).is_source_url("grib", "http://localhost:7117/dataset/test200"));

    mdc.clear();
    mdc.add(*testds, Matcher::parse("origin:GRIB1,80"));
    wassert(actual(mdc.size()) == 0u);

    mdc.clear();
    mdc.add(*testds, Matcher::parse("origin:GRIB1,98"));
    wassert(actual(mdc.size()) == 0u);
});

// Test querying the datasets, with inline data
add_method("inline", [] {
    auto config = dataset::http::Reader::load_cfg_sections("http://localhost:7117");

    unique_ptr<dataset::Reader> testds(dataset::Reader::create(*config.section("test200")));
    metadata::Collection mdc(*testds, dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), true));
    wassert(actual(mdc.size()) == 1u);

    // Check that the source record that comes out is ok
    wassert(actual_type(mdc[0].source()).is_source_inline("grib", 7218));
    wassert(actual(mdc[0].get_data().size()) == 7218u);

    mdc.clear();
    mdc.add(*testds, dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), true));
    wassert(actual(mdc.size()) == 0u);

    // Try again, but let ProcessorMaker build the query
    sys::Tempfile output;
    runtime::ProcessorMaker pm;
    pm.data_inline = true;
    unique_ptr<runtime::DatasetProcessor> proc = pm.make(Matcher::parse("origin:GRIB1,200"), output);
    proc->process(*testds, "test200");
    proc->end();
    mdc.clear();
    Metadata::read_file(output.name(), mdc.inserter_func());
    wassert(actual(mdc.size()) == 1u);
    wassert(actual_type(mdc[0].source()).is_source_inline("grib", 7218));
    wassert(actual(mdc[0].get_data().size()) == 7218u);
});

// Test querying the summary
add_method("summary", [] {
    auto config = dataset::http::Reader::load_cfg_sections("http://localhost:7117");

    unique_ptr<dataset::Reader> testds(dataset::Reader::create(*config.section("test200")));

    Summary summary;
    testds->query_summary(Matcher::parse("origin:GRIB1,200"), summary);
    wassert(actual(summary.count()) == 1u);
});

// Test querying with postprocessing
add_method("postprocess", [] {
    auto config = dataset::http::Reader::load_cfg_sections("http://localhost:7117");

    unique_ptr<dataset::Reader> testds(dataset::Reader::create(*config.section("test200")));

    wassert(actual(dynamic_cast<dataset::http::Reader*>(testds.get())).istrue());

    sys::File out(sys::File::mkstemp("test"));
    dataset::ByteQuery bq;
    bq.setPostprocess(Matcher::parse("origin:GRIB1,200"), "say ciao");
    testds->query_bytes(bq, out);
    out.close();
    string res = sys::read_file(out.name());
    wassert(actual(res) == "ciao\n");
});

// Test the server giving an error
add_method("error", [] {
    auto config = dataset::http::Reader::load_cfg_sections("http://localhost:7117");
    unique_ptr<dataset::Reader> testds(dataset::Reader::create(*config.section("test200")));

    dataset::http::Reader* htd = dynamic_cast<dataset::http::Reader*>(testds.get());
    wassert(actual(htd).istrue());

    // Try it on metadata
    metadata::Collection mdc;
    htd->produce_one_wrong_query();
    try {
        mdc.add(*testds, Matcher::parse("origin:GRIB1,200"));
        ensure(false);
    } catch (std::exception& e) {
        wassert(actual(e.what()).not_contains("<html>"));
        wassert(actual(e.what()).contains("subexpression 'MISCHIEF' does not contain a colon"));
    }
    wassert(actual(mdc.size()) == 0u);

    // Try it on summaries
    Summary summary;
    htd->produce_one_wrong_query();
    try {
        testds->query_summary(Matcher::parse("origin:GRIB1,200"), summary);
        ensure(false);
    } catch (std::exception& e) {}
    wassert(actual(summary.count()) == 0u);

    // Try it on streams
    sys::File out(sys::File::mkstemp("test"));
    htd->produce_one_wrong_query();
    try {
        dataset::ByteQuery bq;
        bq.setPostprocess(Matcher::parse("origin:GRIB1,200"), "say ciao");
        testds->query_bytes(bq, out);
        ensure(false);
    } catch (std::exception& e) {}
    out.close();
    wassert(actual(sys::size(out.name())) == 0u);
});

// Test expanding a query
add_method("qexpand", [] {
    wassert(actual(dataset::http::Reader::expandMatcher("origin:GRIB1,200;product:t", "http://localhost:7117")) ==
          "origin:GRIB1,200; product:GRIB1,200,2,11 or GRIB1,98,128,130 or GRIB1,98,128,167 or GRIB1,200,200,11 or GRIB2,200,0,200,11");
    try {
    dataset::http::Reader::expandMatcher("origin:GRIB1,200;product:pippo", "http://localhost:7117");
        ensure(false);
    } catch (std::runtime_error& e) {
        wassert(actual(e.what()).contains("pippo"));
    }
});

// Test querying the datasets via macro
add_method("qmacro", [] {
    core::cfg::Section cfg;
    cfg.set("name", "noop");
    cfg.set("type", "remote");
    cfg.set("path", "http://localhost:7117");
    cfg.set("qmacro", "test200");
    unique_ptr<dataset::Reader> testds(dataset::Reader::create(cfg));
    metadata::Collection mdc(*testds, Matcher());
    wassert(actual(mdc.size()) == 1u);
    // Check that the source record that comes out is ok
    wassert(actual_type(mdc[0].source()).is_source_url("grib", "http://localhost:7117/query"));
});

// Test querying the datasets via macro
add_method("expa", [] {
    core::cfg::Section cfg;
    cfg.set("name", "expa 2007-07-08");
    cfg.set("type", "remote");
    cfg.set("path", "http://localhost:7117/");
    cfg.set("qmacro", "ds:test200. d:@. t:1300. s:GRIB1/0/0h/0h. l:GRIB1/1. v:GRIB1/200/140/229.");
    unique_ptr<dataset::Reader> testds(dataset::Reader::create(cfg));
    metadata::Collection mdc(*testds, Matcher());
    wassert(actual(mdc.size()) == 1u);
    // Check that the source record that comes out is ok
    wassert(actual_type(mdc[0].source()).is_source_url("grib", "http://localhost:7117/query"));
});

// Test querying the summary
add_method("global_summary", [] {
    core::cfg::Section cfg;
    cfg.set("name", "noop");
    cfg.set("type", "remote");
    cfg.set("path", "http://localhost:7117");
    cfg.set("qmacro", "test200");
    unique_ptr<dataset::Reader> testds(dataset::Reader::create(cfg));

    Summary summary;
    testds->query_summary(Matcher(), summary);
    wassert(actual(summary.count()) == 1u);
});

// Test a postprocessor that outputs data and then exits with error
add_method("postproc_error", [] {
    auto cfg = dataset::http::Reader::load_cfg_section("http://localhost:7117/dataset/test200");
    unique_ptr<dataset::Reader> testds(dataset::Reader::create(cfg));

    // Querying it should get the partial output and no error
    sys::File out(sys::File::mkstemp("test"));
    dataset::ByteQuery bq;
    bq.setPostprocess(Matcher(), "error");
    try {
        testds->query_bytes(bq, out);
        // The call should fail
        wassert(actual(false).istrue());
    } catch (std::runtime_error& e) {
        wassert(actual(e.what()).contains("FAIL"));
    }
    out.close();
    wassert(actual(sys::size(out.name())) == 0u);
});

// Test a postprocessor that outputs data and then exits with error
add_method("postproc_outthenerr", [] {
    auto cfg = dataset::http::Reader::load_cfg_section("http://localhost:7117/dataset/test200");
    unique_ptr<dataset::Reader> testds(dataset::Reader::create(cfg));

    // Querying it should get the partial output and no error
    sys::File out(sys::File::mkstemp("test"));
    dataset::ByteQuery bq;
    bq.setPostprocess(Matcher(), "outthenerr");
    testds->query_bytes(bq, out);
    out.close();
    string res = sys::read_file(out.name());
    wassert(actual(res).contains("So far, so good"));

    // The postprocessor stderr should not appear
    wassert(actual(res).not_contains("Argh"));

    // And we should not get a server error after the normal stream has started
    wassert(actual(res).not_contains("500 Server error"));
});

// Test data integrity of postprocessed queries through a server (used to fail
// after offset 0xc00)
add_method("postproc_error1", [] {
    using namespace arki::dataset;

    // Get the normal data
    vector<uint8_t> plain;
    {
        core::cfg::Section cfg;
        cfg.set("type", "ondisk2");
        cfg.set("path", "test80");
        cfg.set("name", "test80");
        cfg.set("step", "daily");
        cfg.set("filter", "origin:GRIB1,80");
        cfg.set("postprocess", "cat,echo,say,checkfiles,error,outthenerr");
        unique_ptr<dataset::Reader> ds(dataset::Reader::create(cfg));

        DataQuery dq(Matcher::parse(""), true);
        BinaryEncoder enc(plain);
        ds->query_data(dq, [&](unique_ptr<Metadata> md) {
            md->makeInline();
            md->encodeBinary(enc);
            enc.add_raw(md->get_data().read());
            return true;
        });
    }

    // Capture the data after going through the postprocessor
    string postprocessed;
    {
        auto config = dataset::http::Reader::load_cfg_sections("http://localhost:7117");

        sys::File out(sys::File::mkstemp("test"));
        unique_ptr<dataset::Reader> testds(dataset::Reader::create(*config.section("test80")));
        wassert(actual(dynamic_cast<dataset::http::Reader*>(testds.get())).istrue());

        dataset::ByteQuery bq;
        bq.setPostprocess(Matcher::parse("origin:GRIB1,80"), "cat");
        testds->query_bytes(bq, out);
        out.close();
        postprocessed = sys::read_file(out.name());
    }

    wassert(actual(plain.size()) == postprocessed.size());

    /*
    size_t diffs = 0;
    for (size_t i = 0; i < plain.size(); ++i)
    {
        if (plain[i] != postprocessed.str()[i])
        {
            fprintf(stderr, "Difference at offset %d %x %u!=%u\n", (int)i, (int)i, (int)plain[i], (int)postprocessed.str()[i]);
            if (++diffs > 20)
                break;
        }
    }

    ensure(plain == postprocessed.str());
    */

    metadata::Collection mdc1, mdc2;
    {
        Metadata::read_buffer(plain, metadata::ReadContext("", "plain"), mdc1.inserter_func());
    }
    {
        vector<uint8_t> buf(postprocessed.begin(), postprocessed.end());
        Metadata::read_buffer(buf, metadata::ReadContext("", "postprocessed"), mdc2.inserter_func());
    }

    // Remove those metadata that contain test-dependent timestamps
    std::vector<Note> nonotes;
    mdc1[0].unset(TYPE_ASSIGNEDDATASET);
    mdc2[0].unset(TYPE_ASSIGNEDDATASET);
    mdc1[0].set_notes(nonotes);
    mdc2[0].set_notes(nonotes);

    // Compare YAML versions so we get readable output
    string yaml1, yaml2;
    {
        stringstream s1;
        mdc1[0].write_yaml(s1);
        yaml1 = s1.str();
    }
    {
        stringstream s1;
        mdc2[0].write_yaml(s1);
        yaml2 = s1.str();
    }
    wassert(actual(yaml1) == yaml2);

    // Compare data
    const auto& d1 = mdc1[0].get_data().read();
    const auto& d2 = mdc2[0].get_data().read();

    wassert(actual(d1) == d2);
});

// Test downloading the server alias database
add_method("aliases", [] {
    auto cfg = dataset::http::Reader::getAliasDatabase("http://localhost:7117");
    wassert(actual(cfg.section("origin") != nullptr).istrue());
});

// Test uploading postprocessor data
add_method("postproc_data", [] {
    auto cfg = dataset::http::Reader::load_cfg_section("http://localhost:7117/dataset/test200");
    unique_ptr<dataset::Reader> testds(dataset::Reader::create(cfg));
    setenv("ARKI_POSTPROC_FILES", "inbound/test.grib1:inbound/padded.grib1", 1);

    // Files should be uploaded and notified to the postprocessor script
    sys::File out(sys::File::mkstemp("test"));
    dataset::ByteQuery bq;
    bq.setPostprocess(Matcher(), "checkfiles");
    testds->query_bytes(bq, out);
    out.close();
    string res = sys::read_file(out.name());
    wassert(actual(res) == "padded.grib1:test.grib1\n");
});

// Test access and error logs
add_method("logs", [] {
    auto cfg = dataset::http::Reader::load_cfg_section("http://localhost:7117/dataset/test200");
    unique_ptr<dataset::Reader> testds(dataset::Reader::create(cfg));

    // Run a successful query
    testds->query_data(Matcher(), [](unique_ptr<Metadata> md) { return true; });

    // Run a broken query
    dynamic_cast<dataset::http::Reader*>(testds.get())->produce_one_wrong_query();
    try {
        testds->query_data(Matcher(), [](unique_ptr<Metadata> md) { return true; });
        wassert(actual(false).istrue());
    } catch (std::runtime_error& e) {
        wassert(actual(e.what()).contains("MISCHIEF"));
    }

    // Check that something got logged
    wassert(actual(sys::size("access.log")) > 0u);
    wassert(actual(sys::size("error.log")) > 0u);
});

// Test style=binary summary queries
add_method("query_global_summary_style_binary", [] {
    dataset::http::CurlEasy curl;

    dataset::http::BufState<std::string> request(curl);
    request.set_url("http://localhost:7117/summary?style=binary");
    request.set_method("POST");
    request.post_data.add_string("query", Matcher().toStringExpanded());
    request.perform();

    wassert(actual(request.buf).startswith("SU"));
});

// Test style=binary summary queries
add_method("query_summary_style_binary", [] {
    dataset::http::CurlEasy curl;

    dataset::http::BufState<std::string> request(curl);
    request.set_url("http://localhost:7117/dataset/test200/summary?style=binary");
    request.set_method("POST");
    request.post_data.add_string("query", Matcher().toStringExpanded());
    request.perform();

    wassert(actual(request.buf).startswith("SU"));
});

// Test style=json summary queries
add_method("query_summary_style_json", [] {
    dataset::http::CurlEasy curl;

    dataset::http::BufState<std::string> request(curl);
    request.set_url("http://localhost:7117/dataset/test200/summary?style=json");
    request.set_method("POST");
    request.post_data.add_string("query", Matcher().toStringExpanded());
    request.perform();

    wassert(actual(request.buf).startswith("{"));
});

// Test style=yaml summary queries
add_method("query_summary_style_yaml", [] {
    dataset::http::CurlEasy curl;

    dataset::http::BufState<std::string> request(curl);
    request.set_url("http://localhost:7117/dataset/test200/summary?style=yaml");
    request.set_method("POST");
    request.post_data.add_string("query", Matcher().toStringExpanded());
    request.perform();

    wassert(actual(request.buf).startswith("SummaryItem"));
});

// Test style=json summary queries
add_method("query_summaryshort_style_json", [] {
    dataset::http::CurlEasy curl;

    dataset::http::BufState<std::string> request(curl);
    request.set_url("http://localhost:7117/dataset/test200/summaryshort?style=json");
    request.set_method("POST");
    request.post_data.add_string("query", Matcher().toStringExpanded());
    request.perform();

    wassert(actual(request.buf).startswith("{"));
});

// Test style=yaml summary queries
add_method("query_summaryshort_style_yaml", [] {
    dataset::http::CurlEasy curl;

    dataset::http::BufState<std::string> request(curl);
    request.set_url("http://localhost:7117/dataset/test200/summaryshort?style=yaml");
    request.set_method("POST");
    request.post_data.add_string("query", Matcher().toStringExpanded());
    request.perform();

    wassert(actual(request.buf).startswith("SummaryStats"));
});

}

}
