/*
 * Copyright (C) 2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <arki/tests/test-utils.h>
#include <arki/scan/any.h>
#include <arki/types.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/types/level.h>
#include <arki/types/timerange.h>
#include <arki/types/reftime.h>
#include <arki/types/area.h>
#include <arki/types/ensemble.h>
#include <arki/types/run.h>
#include <arki/metadata.h>
#include <arki/utils/metadata.h>
#include <wibble/sys/fs.h>

#include "config.h"

#ifdef HAVE_DBALLE
#include <dballe++/init.h>
#endif

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;

struct arki_scan_any_shar {
#ifdef HAVE_DBALLE
	dballe::DballeInit dballeInit;
#endif
};
TESTGRP(arki_scan_any);

// Scan a well-known grib file, with no padding between messages
template<> template<>
void to::test<1>()
{
	utils::metadata::Collector mdc;
#ifndef HAVE_GRIBAPI
	ensure(not scan::scan("inbound/test.grib1", mdc));
#else
	wibble::sys::Buffer buf;
	ValueBag vb;

	ensure(scan::scan("inbound/test.grib1", mdc));

	ensure_equals(mdc.size(), 3u);

	// Check the source info
	ensure_equals(mdc[0].source, Item<Source>(source::Blob::create("grib1", sys::fs::abspath("inbound/test.grib1"), 0, 7218)));

	// Check that the source can be read properly
	buf = mdc[0].getData();
	ensure_equals(buf.size(), 7218u);
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 7214, 4), "7777");

	// Check origin
	ensure(mdc[0].get(types::TYPE_ORIGIN).defined());
	ensure_equals(mdc[0].get(types::TYPE_ORIGIN), Item<>(origin::GRIB1::create(200, 0, 101)));

	// Check product
	ensure(mdc[0].get(types::TYPE_PRODUCT).defined());
	ensure_equals(mdc[0].get(types::TYPE_PRODUCT), Item<>(product::GRIB1::create(200, 140, 229)));

	// Check level
	ensure(mdc[0].get(types::TYPE_LEVEL).defined());
	ensure_equals(mdc[0].get(types::TYPE_LEVEL), Item<>(level::GRIB1::create(1, 0)));

	// Check timerange
	ensure(mdc[0].get(types::TYPE_TIMERANGE).defined());
	ensure_equals(mdc[0].get(types::TYPE_TIMERANGE), Item<>(timerange::GRIB1::create(0, 254, 0, 0)));

	// Check area
	vb.clear();
	vb.set("Ni", Value::createInteger(97));
	vb.set("Nj", Value::createInteger(73));
	vb.set("latfirst", Value::createInteger(40000));
	vb.set("latlast", Value::createInteger(46000));
	vb.set("lonfirst", Value::createInteger(12000));
	vb.set("lonlast", Value::createInteger(20000));
	vb.set("type", Value::createInteger(0));
	ensure(mdc[0].get(types::TYPE_AREA).defined());
	ensure_equals(mdc[0].get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

	// Check ensemble
	vb.clear();
	ensure(!mdc[0].get(types::TYPE_ENSEMBLE).defined());
	//ensure_equals(md.get(types::TYPE_ENSEMBLE), ensemble::GRIB::create(vb));

	// Check reftime
	ensure_equals(mdc[0].get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(mdc[0].get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2007, 7, 8, 13, 0, 0))));

	// Check run
	ensure_equals(mdc[0].get(types::TYPE_RUN).upcast<Run>()->style(), Run::MINUTE);
	ensure_equals(mdc[0].get(types::TYPE_RUN), Item<>(run::Minute::create(13)));


	// Check the source info
	ensure_equals(mdc[1].source, Item<Source>(source::Blob::create("grib1", sys::fs::abspath("inbound/test.grib1"), 7218, 34960)));

	// Check that the source can be read properly
	buf = mdc[1].getData();
	ensure_equals(buf.size(), 34960u);
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 34956, 4), "7777");

	// Check origin
	ensure(mdc[1].get(types::TYPE_ORIGIN).defined());
	ensure_equals(mdc[1].get(types::TYPE_ORIGIN), Item<>(origin::GRIB1::create(80, 255, 100)));

	// Check product
	ensure(mdc[1].get(types::TYPE_PRODUCT).defined());
	ensure_equals(mdc[1].get(types::TYPE_PRODUCT), Item<>(product::GRIB1::create(80, 2, 2)));

	// Check level
	ensure(mdc[1].get(types::TYPE_LEVEL).defined());
	ensure_equals(mdc[1].get(types::TYPE_LEVEL), Item<>(level::GRIB1::create(102, 0)));

	// Check timerange
	ensure(mdc[1].get(types::TYPE_TIMERANGE).defined());
	ensure_equals(mdc[1].get(types::TYPE_TIMERANGE), Item<>(timerange::GRIB1::create(1, 254, 0, 0)));

	// Check area
	vb.clear();
	vb.set("Ni", Value::createInteger(205));
	vb.set("Nj", Value::createInteger(85));
	vb.set("latfirst", Value::createInteger(30000));
	vb.set("latlast", Value::createInteger(72000));
	vb.set("lonfirst", Value::createInteger(-60000));
	vb.set("lonlast", Value::createInteger(42000));
	vb.set("type", Value::createInteger(0));
	ensure(mdc[1].get(types::TYPE_AREA).defined());
	ensure_equals(mdc[1].get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

	// Check ensemble
	vb.clear();
	ensure(!mdc[1].get(types::TYPE_ENSEMBLE).defined());
	//ensure_equals(md.get(types::TYPE_ENSEMBLE), ensemble::GRIB::create(vb));

	// Check reftime
	ensure_equals(mdc[1].get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(mdc[1].get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2007, 7, 7, 0, 0, 0))));

	// Check run
	ensure_equals(mdc[1].get(types::TYPE_RUN).upcast<Run>()->style(), Run::MINUTE);
	ensure_equals(mdc[1].get(types::TYPE_RUN), Item<>(run::Minute::create(0)));


	// Check the source info
	ensure_equals(mdc[2].source, Item<Source>(source::Blob::create("grib1", sys::fs::abspath("inbound/test.grib1"), 42178, 2234)));

	// Check that the source can be read properly
	buf = mdc[2].getData();
	ensure_equals(buf.size(), 2234u);
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 2230, 4), "7777");

	// Check origin
	ensure(mdc[2].get(types::TYPE_ORIGIN).defined());
	ensure_equals(mdc[2].get(types::TYPE_ORIGIN), Item<>(origin::GRIB1::create(98, 0, 129)));

	// Check product
	ensure(mdc[2].get(types::TYPE_PRODUCT).defined());
	ensure_equals(mdc[2].get(types::TYPE_PRODUCT), Item<>(product::GRIB1::create(98, 128, 129)));

	// Check level
	ensure(mdc[2].get(types::TYPE_LEVEL).defined());
	ensure_equals(mdc[2].get(types::TYPE_LEVEL), Item<>(level::GRIB1::create(100, 1000)));

	// Check timerange
	ensure(mdc[2].get(types::TYPE_TIMERANGE).defined());
	ensure_equals(mdc[2].get(types::TYPE_TIMERANGE), Item<>(timerange::GRIB1::create(0, 254, 0, 0)));

	// Check area
	vb.clear();
	vb.set("Ni", Value::createInteger(43));
	vb.set("Nj", Value::createInteger(25));
	vb.set("latfirst", Value::createInteger(55500));
	vb.set("latlast", Value::createInteger(31500));
	vb.set("lonfirst", Value::createInteger(-11500));
	vb.set("lonlast", Value::createInteger(30500));
	vb.set("type", Value::createInteger(0));
	ensure(mdc[2].get(types::TYPE_AREA).defined());
	ensure_equals(mdc[2].get(types::TYPE_AREA), Item<>(area::GRIB::create(vb)));

	// Check ensemble
	vb.clear();
	ensure(!mdc[2].get(types::TYPE_ENSEMBLE).defined());
	//ensure_equals(md.get(types::TYPE_ENSEMBLE), ensemble::GRIB::create(vb));

	// Check reftime
	ensure_equals(mdc[2].get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(mdc[2].get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2007, 10, 9, 0, 0, 0))));

	// Check run
	ensure_equals(mdc[2].get(types::TYPE_RUN).upcast<Run>()->style(), Run::MINUTE);
	ensure_equals(mdc[2].get(types::TYPE_RUN), Item<>(run::Minute::create(0)));
#endif
}

// Scan a well-known bufr file, with no padding between BUFRs
template<> template<>
void to::test<2>()
{
	utils::metadata::Collector mdc;
#ifndef HAVE_DBALLE
	ensure(not scan::scan("inbound/test.bufr", mdc));
#else
	wibble::sys::Buffer buf;

	ensure(scan::scan("inbound/test.bufr", mdc));

	ensure_equals(mdc.size(), 3u);

	// Check the source info
	ensure_equals(mdc[0].source, Item<Source>(source::Blob::create("bufr", sys::fs::abspath("inbound/test.bufr"), 0, 194)));

	// Check that the source can be read properly
	buf = mdc[0].getData();
	ensure_equals(buf.size(), 194u);
	ensure_equals(string((const char*)buf.data(), 4), "BUFR");
	ensure_equals(string((const char*)buf.data() + 190, 4), "7777");

	// Check origin
	ensure(mdc[0].get(types::TYPE_ORIGIN).defined());
	ensure_equals(mdc[0].get(types::TYPE_ORIGIN), Item<>(origin::BUFR::create(98, 0)));

	// Check product
	ensure(mdc[0].get(types::TYPE_PRODUCT).defined());
	ensure_equals(mdc[0].get(types::TYPE_PRODUCT), Item<>(product::BUFR::create(0, 255, 1)));

	// Check reftime
	ensure_equals(mdc[0].get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(mdc[0].get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2005, 12, 1, 18, 0, 0))));

	// Check run
	ensure_equals(mdc[0].get(types::TYPE_RUN).upcast<Run>()->style(), Run::MINUTE);
	ensure_equals(mdc[0].get(types::TYPE_RUN), Item<>(run::Minute::create(18)));


	// Check the source info
	ensure_equals(mdc[1].source, Item<Source>(source::Blob::create("bufr", sys::fs::abspath("inbound/test.bufr"), 194, 220)));

	// Check that the source can be read properly
	buf = mdc[1].getData();
	ensure_equals(buf.size(), 220u);
	ensure_equals(string((const char*)buf.data(), 4), "BUFR");
	ensure_equals(string((const char*)buf.data() + 216, 4), "7777");

	// Check origin
	ensure(mdc[1].get(types::TYPE_ORIGIN).defined());
	ensure_equals(mdc[1].get(types::TYPE_ORIGIN), Item<>(origin::BUFR::create(98, 0)));

	// Check product
	ensure(mdc[1].get(types::TYPE_PRODUCT).defined());
	ensure_equals(mdc[1].get(types::TYPE_PRODUCT), Item<>(product::BUFR::create(0, 255, 1)));

	// Check reftime
	ensure_equals(mdc[1].get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(mdc[1].get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2004, 11, 30, 12, 0, 0))));

	// Check run
	ensure_equals(mdc[1].get(types::TYPE_RUN).upcast<Run>()->style(), Run::MINUTE);
	ensure_equals(mdc[1].get(types::TYPE_RUN), Item<>(run::Minute::create(12)));


	// Check the source info
	ensure_equals(mdc[2].source, Item<Source>(source::Blob::create("bufr", sys::fs::abspath("inbound/test.bufr"), 414, 220)));

	// Check that the source can be read properly
	buf = mdc[2].getData();
	ensure_equals(buf.size(), 220u);
	ensure_equals(string((const char*)buf.data(), 4), "BUFR");
	ensure_equals(string((const char*)buf.data() + 216, 4), "7777");

	// Check origin
	ensure(mdc[2].get(types::TYPE_ORIGIN).defined());
	ensure_equals(mdc[2].get(types::TYPE_ORIGIN), Item<>(origin::BUFR::create(98, 0)));

	// Check product
	ensure(mdc[2].get(types::TYPE_PRODUCT).defined());
	ensure_equals(mdc[2].get(types::TYPE_PRODUCT), Item<>(product::BUFR::create(0, 255, 3)));

	// Check reftime
	ensure_equals(mdc[2].get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(mdc[2].get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2004, 11, 30, 12, 0, 0))));

	// Check run
	ensure_equals(mdc[2].get(types::TYPE_RUN).upcast<Run>()->style(), Run::MINUTE);
	ensure_equals(mdc[2].get(types::TYPE_RUN), Item<>(run::Minute::create(12)));
#endif
}

}

// vim:set ts=4 sw=4:
