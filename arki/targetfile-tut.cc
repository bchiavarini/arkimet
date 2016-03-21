#include <arki/tests/tests.h>
#include <arki/targetfile.h>
#include <arki/metadata.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/types/level.h>
#include <arki/types/timerange.h>
#include <arki/types/area.h>
#include <arki/types/proddef.h>
#include <arki/types/run.h>
#include <arki/types/reftime.h>

#include <sstream>
#include <iostream>
#include <memory>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::tests;
using arki::core::Time;

struct arki_targetfile_shar {
	Targetfile tf;
	Metadata md;

	arki_targetfile_shar()
		: tf(
			"targetfile['echo'] = function(format)\n"
			"  return function(md)\n"
			"    return format\n"
			"  end\n"
			"end\n"
		)
	{
		tf.loadRCFiles();
		using namespace arki::types;
		ValueBag testValues;
		testValues.set("antani", Value::createInteger(-1));
		testValues.set("blinda", Value::createInteger(0));

        md.set(origin::GRIB1::create(1, 2, 3));
        md.set(product::GRIB1::create(1, 2, 3));
        md.set(level::GRIB1::create(110, 12, 13));
        md.set(timerange::GRIB1::create(0, 0, 0, 0));
        md.set(area::GRIB::create(testValues));
        md.set(proddef::GRIB::create(testValues));
        md.add_note("test note");
        md.set(run::Minute::create(12));
        md.set(reftime::Position::create(Time(2007, 1, 2, 3, 4, 5)));
    }
};
TESTGRP(arki_targetfile);

// Empty or unsupported area should give 0
def_test(1)
{
	Targetfile::Func f = tf.get("echo:foo");
	ensure_equals(f(md), "foo");
}

// Test MARS expansion
def_test(2)
{
	Targetfile::Func f = tf.get("mars:foo[DATE][TIME]+[STEP].grib");
	ensure_equals(f(md), "foo200701020304+00.grib");
}

}

// vim:set ts=4 sw=4:
