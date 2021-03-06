#include "tests.h"
#include "origin.h"

namespace {
using namespace std;
using namespace arki::tests;
using namespace arki;

class Tests : public TypeTestCase<types::Origin>
{
    using TypeTestCase::TypeTestCase;
    void register_tests() override;
} test("arki_types_origin");

void Tests::register_tests() {

// Check GRIB1
add_generic_test(
        "grib1",
        { "GRIB1(1, 1, 1)", "GRIB1(1, 2, 2)", },
        "GRIB1(1, 2, 3)",
        { "GRIB1(1, 2, 4)", "GRIB1(2, 3, 4)", "GRIB2(1, 2, 3, 4, 5)", "BUFR(1, 2)", },
        "GRIB1,1,2,3");

add_method("grib1_details", [] {
    using namespace arki::types;
    unique_ptr<Origin> o = Origin::createGRIB1(1, 2, 3);
    wassert(actual(o->style()) == Origin::Style::GRIB1);
    const origin::GRIB1* v = dynamic_cast<origin::GRIB1*>(o.get());
    wassert(actual(v->centre()) == 1u);
    wassert(actual(v->subcentre()) == 2u);
    wassert(actual(v->process()) == 3u);
});

// Check GRIB2
add_generic_test(
    "grib2",
    { "GRIB1(1, 2, 3)", "GRIB2(1, 2, 3, 4, 4)", },
    "GRIB2(1, 2, 3, 4, 5)",
    { "GRIB2(1, 2, 3, 4, 6)", "GRIB2(2, 3, 4, 5, 6)", "BUFR(1, 2)", },
    "GRIB2,1,2,3,4,5");

add_method("grib2_details", [] {
    using namespace arki::types;
    unique_ptr<Origin> o = Origin::createGRIB2(1, 2, 3, 4, 5);
    wassert(actual(o->style()) == Origin::Style::GRIB2);
    const origin::GRIB2* v = dynamic_cast<origin::GRIB2*>(o.get());
    wassert(actual(v->centre()) == 1u);
    wassert(actual(v->subcentre()) == 2u);
    wassert(actual(v->processtype()) == 3u);
    wassert(actual(v->bgprocessid()) == 4u);
    wassert(actual(v->processid()) == 5u);
});

// Check BUFR
add_generic_test(
    "bufr",
    { "GRIB1(1, 2, 3)", "GRIB2(1, 2, 3, 4, 5)", "BUFR(0, 2)", "BUFR(1, 1)", },
    "BUFR(1, 2)",
    { "BUFR(1, 3)", "BUFR(2, 1)", },
    "BUFR,1,2");

add_method("bufr_details", [] {
    using namespace arki::types;
    unique_ptr<Origin> o = Origin::createBUFR(1, 2);
    wassert(actual(o->style()) == Origin::Style::BUFR);
    const origin::BUFR* v = dynamic_cast<origin::BUFR*>(o.get());
    wassert(actual(v->centre()) == 1u);
    wassert(actual(v->subcentre()) == 2u);
});

// Check ODIMH5
add_generic_test(
    "odimh5",
    { "GRIB1(1, 2, 3)", "GRIB2(1, 2, 3, 4, 5)", "BUFR(0, 2)", },
    "ODIMH5(1, 2, 3)",
    { "ODIMH5(1a, 2, 3)", "ODIMH5(1, 2a, 3)", "ODIMH5(1, 2, 3a)", "ODIMH5(1a, 2a, 3a)", },
    "ODIMH5,1,2,3");

// Check ODIMH5 with empty strings
add_generic_test(
    "odimh5_empty",
    { "GRIB1(1, 2, 3)", "GRIB2(1, 2, 3, 4, 5)", "BUFR(0, 2)", },
    "ODIMH5(, 2, 3)",
    { "ODIMH5(1a, 2, 3)", "ODIMH5(1, 2a, 3)", "ODIMH5(1, 2, 3a)", "ODIMH5(1a, 2a, 3a)", },
    "ODIMH5,,2,3");

add_method("odim_details", [] {
    using namespace arki::types;
    unique_ptr<Origin> o = Origin::createODIMH5("1", "2", "3");
    wassert(actual(o->style()) == Origin::Style::ODIMH5);
    const origin::ODIMH5* v = dynamic_cast<origin::ODIMH5*>(o.get());
    wassert(actual(v->getWMO()) == "1");
    wassert(actual(v->getRAD()) == "2");
    wassert(actual(v->getPLC()) == "3");

    o = Origin::createODIMH5("", "2", "3");
    wassert(actual(o->style()) == Origin::Style::ODIMH5);
    v = dynamic_cast<origin::ODIMH5*>(o.get());
    wassert(actual(v->getWMO()) == "");
    wassert(actual(v->getRAD()) == "2");
    wassert(actual(v->getPLC()) == "3");
});

}

}
