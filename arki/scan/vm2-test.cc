#include <arki/metadata/tests.h>
#include <arki/metadata/collection.h>
#include <arki/types/value.h>
#include <arki/scan/vm2.h>
#include <arki/scan/any.h>
#include <arki/utils/sys.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <fstream>

namespace {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

def_tests(arki_scan_vm2);

void Tests::register_tests() {

// Scan a well-known vm2 sample
add_method("scan", []() {
    Metadata md;
    scan::Vm2 scanner;
    vector<uint8_t> buf;

    scanner.open("inbound/test.vm2");
    // See how we scan the first vm2
    ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("vm2", sys::abspath("."), "inbound/test.vm2", 0, 34));

    // Check contents
    wassert(actual(md).contains("area", "VM2(1)"));
    wassert(actual(md).contains("product", "VM2(227)"));
    wassert(actual(md).contains("reftime", "1987-10-31T00:00:00Z"));
    wassert(actual(md).contains("value", "1.2,,,000000000"));

    // Check that the source can be read properly
    md.unset(TYPE_VALUE);
    md.drop_cached_data();
    buf = md.getData();
    ensure_equals(buf.size(), 34u);
    ensure_equals(string((const char*)buf.data(), 34), "198710310000,1,227,1.2,,,000000000");
});

// Scan a well-known vm2 sample (with seconds)
add_method("scan_seconds", []() {
    Metadata md;
    scan::Vm2 scanner;
    vector<uint8_t> buf;

    scanner.open("inbound/test.vm2");
    // Skip the first vm2
    wassert(actual(scanner.next(md)));

    // See how we scan the second vm2
    wassert(actual(scanner.next(md)));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("vm2", sys::abspath("."), "inbound/test.vm2", 35, 35));

    // Check contents
    wassert(actual(md).contains("area", "VM2(1)"));
    wassert(actual(md).contains("product", "VM2(228)"));
    wassert(actual(md).contains("reftime", "1987-10-31T00:00:30Z"));
    wassert(actual(md).contains("value", ".5,,,000000000"));

    // Check that the source can be read properly
    md.unset(TYPE_VALUE);
    md.drop_cached_data();
    buf = md.getData();
    wassert(actual(buf.size()) == 35u);
    wassert(actual(string((const char*)buf.data(), 35)) == "19871031000030,1,228,.5,,,000000000");
});

add_method("validate", []() {
    Metadata md;
    vector<uint8_t> buf;

    const scan::Validator& v = scan::vm2::validator();

    sys::File fd("inbound/test.vm2", O_RDONLY);
#define ensure_no_throws(x) do { try { x; } catch(std::exception&) { ensure(false); } } while (0)
#define ensure_throws(x) do { try { x; ensure(false); } catch (std::exception&) { } } while (0)
    ensure_no_throws(v.validate_file(fd, 0, 35));
    ensure_no_throws(v.validate_file(fd, 0, 34));
    ensure_no_throws(v.validate_file(fd, 35, 35));
    ensure_no_throws(v.validate_file(fd, 35, 36));

    ensure_throws(v.validate_file(fd, 1, 35));
    ensure_throws(v.validate_file(fd, 0, 36));
    ensure_throws(v.validate_file(fd, 34, 34));
    ensure_throws(v.validate_file(fd, 36, 34));

    fd.close();

    metadata::Collection mdc;
    scan::scan("inbound/test.vm2", mdc.inserter_func());
    mdc[0].unset(TYPE_VALUE);
    buf = mdc[0].getData();

    v.validate_buf(buf.data(), buf.size());
    ensure_throws(v.validate_buf((const char*)buf.data()+1, buf.size()-1));

    std::ifstream in("inbound/test.vm2");
    std::string line;
    while (std::getline(in, line)) {
        line += "\n";
        ensure_no_throws(v.validate_buf(line.c_str(), line.size()));
    }
});

// Scan and reconstruct a VM2 sample
add_method("reconstruct", []() {
    const types::Value* value;
    vector<uint8_t> buf;

    metadata::Collection mdc;
    scan::scan("inbound/test.vm2", mdc.inserter_func());

    value = mdc[0].get<types::Value>();
    buf = scan::Vm2::reconstruct(mdc[0], value->buffer);
    wassert(actual(string((const char*)buf.data(), buf.size())) == "198710310000,1,227,1.2,,,000000000");

    value = mdc[1].get<types::Value>();
    buf = scan::Vm2::reconstruct(mdc[1], value->buffer);
    wassert(actual(string((const char*)buf.data(), buf.size())) == "19871031000030,1,228,.5,,,000000000");
});

// Scan a corrupted VM2
add_method("corrupted", []() {
    system("cp inbound/test.vm2 inbound/test-corrupted.vm2");
    system("dd if=/dev/zero of=inbound/test-corrupted.vm2 bs=1 seek=71 count=33 conv=notrunc 2>/dev/null");

    metadata::Collection mdc;
    scan::scan("inbound/test-corrupted.vm2", mdc.inserter_func());

    wassert(actual(mdc.size()) == 3u);

    // Check the source info
    wassert(actual(mdc[0].source().cloneType()).is_source_blob("vm2", sys::abspath("."), "inbound/test-corrupted.vm2", 0, 34));
    wassert(actual(mdc[1].source().cloneType()).is_source_blob("vm2", sys::abspath("."), "inbound/test-corrupted.vm2", 35, 35));
    wassert(actual(mdc[2].source().cloneType()).is_source_blob("vm2", sys::abspath("."), "inbound/test-corrupted.vm2", 105, 32));

    system("rm inbound/test-corrupted.vm2");
});

}
}