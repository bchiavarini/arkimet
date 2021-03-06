#include "arki/matcher/tests.h"
#include "arki/matcher.h"
#include "arki/metadata.h"
#include "arki/types/product.h"

using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_matcher_product");

void Tests::register_tests() {

// Try matching GRIB product
add_method("grib", [] {
    Metadata md;
    arki::tests::fill(md);

	ensure_matches("product:GRIB1", md);
	ensure_matches("product:GRIB1,,,", md);
	ensure_matches("product:GRIB1,1,,", md);
	ensure_matches("product:GRIB1,,2,", md);
	ensure_matches("product:GRIB1,,,3", md);
	ensure_matches("product:GRIB1,1,2,", md);
	ensure_matches("product:GRIB1,1,,3", md);
	ensure_matches("product:GRIB1,,2,3", md);
	ensure_matches("product:GRIB1,1,2,3", md);
	ensure_not_matches("product:GRIB1,2", md);
	ensure_not_matches("product:GRIB1,,3", md);
	ensure_not_matches("product:GRIB1,,,1", md);
	ensure_not_matches("product:BUFR,1,2,3", md);

    // If we have more than one product, we match any of them
    md.set(product::GRIB1::create(2, 3, 4));
    ensure_matches("product:GRIB1,2", md);
});

// Try matching BUFR product
add_method("bufr", [] {
    Metadata md;
    arki::tests::fill(md);

    ValueBag vb;
    vb.set("name", values::Value::create_string("antani"));
    md.set(product::BUFR::create(1, 2, 3, vb));

	ensure_matches("product:BUFR", md);
	ensure_matches("product:BUFR,1", md);
	ensure_matches("product:BUFR,1,2", md);
	ensure_matches("product:BUFR,1,2,3", md);
	ensure_matches("product:BUFR,1,2,3:name=antani", md);
	ensure_matches("product:BUFR,1:name=antani", md);
	ensure_matches("product:BUFR:name=antani", md);
	ensure_not_matches("product:BUFR,1,2,3:name=blinda", md);
	ensure_not_matches("product:BUFR,1,2,3:name=antan", md);
	ensure_not_matches("product:BUFR,1,2,3:name=antani1", md);
	ensure_not_matches("product:BUFR,1,2,3:enam=antani", md);
	ensure_not_matches("product:GRIB1,1,2,3", md);
    auto e1 = wassert_throws(std::invalid_argument, Matcher::parse("product:BUFR,name=antani"));
    wassert(actual(e1.what()).contains("is not a number"));
    auto e2 = wassert_throws(std::runtime_error, Matcher::parse("product:BUFR:0,,2"));
    wassert(actual(e2.what()).contains("key=value"));
});

// Try matching VM2 product
add_method("vm2", [] {
    skip_unless_vm2();
    Metadata md;
    arki::tests::fill(md);
    md.set(product::VM2::create(1));

	ensure_matches("product:VM2", md);
	ensure_matches("product:VM2,", md);
	ensure_matches("product:VM2,1", md);
	ensure_not_matches("product:GRIB1,1,2,3", md);
    ensure_not_matches("product:VM2,2", md);

    auto e1 = wassert_throws(std::invalid_argument, Matcher::parse("product:VM2,ciccio=riccio"));
    wassert(actual(e1.what()).contains("is not a number"));

    ensure_not_matches("product: VM2:ciccio=riccio", md);
    ensure_not_matches("product: VM2,1:ciccio=riccio", md);
    ensure_matches("product: VM2,1:bcode=B20013", md);
});

}

}
