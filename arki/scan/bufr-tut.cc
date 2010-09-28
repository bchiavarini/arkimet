/*
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/scan/bufr.h>
#include <arki/types.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/types/reftime.h>
#include <arki/types/area.h>
#include <arki/types/run.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/scan/any.h>
#include <wibble/sys/fs.h>

#include <sstream>
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace tut {
using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

struct arki_scan_bufr_shar {
};
TESTGRP(arki_scan_bufr);

// Scan a well-known bufr file, with no padding between BUFRs
template<> template<>
void to::test<1>()
{
	Metadata md;
	scan::Bufr scanner;
	types::Time reftime;
	wibble::sys::Buffer buf;

	scanner.open("inbound/test.bufr");

	// See how we scan the first BUFR
	ensure(scanner.next(md));

	// Check the source info
	ensure_equals(md.source, Item<Source>(source::Blob::create("bufr", sys::fs::abspath("inbound/test.bufr"), 0, 194)));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 194u);
	ensure_equals(string((const char*)buf.data(), 4), "BUFR");
	ensure_equals(string((const char*)buf.data() + 190, 4), "7777");

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::BUFR::create(98, 0)));

	// Check product
	ValueBag vb;
	vb.set("t", Value::createString("synop"));
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::BUFR::create(0, 255, 1, vb)));

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2005, 12, 1, 18, 0, 0))));

	// Check area
	Item<area::GRIB> area = md.get(types::TYPE_AREA).upcast<area::GRIB>();
	ensure_equals(area->values().get("lat")->toString(), "4153000");
	ensure_equals(area->values().get("lon")->toString(), "2070000");
	ensure_equals(area->values().get("blo")->toString(), "13");
	ensure_equals(area->values().get("sta")->toString(), "577");

	// Check run
	ensure(not md.has(types::TYPE_RUN));


	// Next bufr
	ensure(scanner.next(md));

	// Check the source info
	ensure_equals(md.source, Item<Source>(source::Blob::create("bufr", sys::fs::abspath("inbound/test.bufr"), 194, 220)));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 220u);
	ensure_equals(string((const char*)buf.data(), 4), "BUFR");
	ensure_equals(string((const char*)buf.data() + 216, 4), "7777");

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::BUFR::create(98, 0)));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::BUFR::create(0, 255, 1, vb)));

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2004, 11, 30, 12, 0, 0))));

	// Check run
	ensure(not md.has(types::TYPE_RUN));


	// Last bufr
	ensure(scanner.next(md));

	// Check the source info
	ensure_equals(md.source, Item<Source>(source::Blob::create("bufr", sys::fs::abspath("inbound/test.bufr"), 414, 220)));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 220u);
	ensure_equals(string((const char*)buf.data(), 4), "BUFR");
	ensure_equals(string((const char*)buf.data() + 216, 4), "7777");

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::BUFR::create(98, 0)));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::BUFR::create(0, 255, 3, vb)));

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2004, 11, 30, 12, 0, 0))));

	// Check run
	ensure(not md.has(types::TYPE_RUN));
	

	// No more bufrs
	ensure(not scanner.next(md));
}


// Scan a well-known bufr file, with extra padding data between messages
template<> template<>
void to::test<2>()
{
	Metadata md;
	scan::Bufr scanner;
	types::Time reftime;
	wibble::sys::Buffer buf;

	scanner.open("inbound/padded.bufr");

	// See how we scan the first BUFR
	ensure(scanner.next(md));

	// Check the source info
	ensure_equals(md.source, Item<Source>(source::Blob::create("bufr", sys::fs::abspath("inbound/padded.bufr"), 100, 194)));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 194u);
	ensure_equals(string((const char*)buf.data(), 4), "BUFR");
	ensure_equals(string((const char*)buf.data() + 190, 4), "7777");

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::BUFR::create(98, 0)));

	// Check product
	ValueBag vb;
	vb.set("t", Value::createString("synop"));
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::BUFR::create(0, 255, 1, vb)));

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2005, 12, 1, 18, 0, 0))));

	// Check run
	ensure(not md.has(types::TYPE_RUN));


	// Next bufr
	ensure(scanner.next(md));

	// Check the source info
	ensure_equals(md.source, Item<Source>(source::Blob::create("bufr", sys::fs::abspath("inbound/padded.bufr"), 394, 220)));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 220u);
	ensure_equals(string((const char*)buf.data(), 4), "BUFR");
	ensure_equals(string((const char*)buf.data() + 216, 4), "7777");

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::BUFR::create(98, 0)));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::BUFR::create(0, 255, 1, vb)));

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2004, 11, 30, 12, 0, 0))));

	// Check run
	ensure(not md.has(types::TYPE_RUN));


	// Last bufr
	ensure(scanner.next(md));

	// Check the source info
	ensure_equals(md.source, Item<Source>(source::Blob::create("bufr", sys::fs::abspath("inbound/padded.bufr"), 714, 220)));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 220u);
	ensure_equals(string((const char*)buf.data(), 4), "BUFR");
	ensure_equals(string((const char*)buf.data() + 216, 4), "7777");

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::BUFR::create(98, 0)));

	// Check product
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::BUFR::create(0, 255, 3, vb)));

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2004, 11, 30, 12, 0, 0))));

	// Check run
	ensure(not md.has(types::TYPE_RUN));
	

	// No more bufrs
	ensure(not scanner.next(md));
}

// Test validation
template<> template<>
void to::test<3>()
{
	Metadata md;
	wibble::sys::Buffer buf;

	const scan::Validator& v = scan::bufr::validator();

	int fd = open("inbound/test.bufr", O_RDONLY);

	v.validate(fd, 0, 194, "inbound/test.bufr");
	v.validate(fd, 194, 220, "inbound/test.bufr");
	v.validate(fd, 414, 220, "inbound/test.bufr");

#define ensure_throws(x) do { try { x; ensure(false); } catch (wibble::exception::Generic& e) { } } while (0)

	ensure_throws(v.validate(fd, 1, 193, "inbound/test.bufr"));
	ensure_throws(v.validate(fd, 0, 193, "inbound/test.bufr"));
	ensure_throws(v.validate(fd, 0, 195, "inbound/test.bufr"));
	ensure_throws(v.validate(fd, 193, 221, "inbound/test.bufr"));
	ensure_throws(v.validate(fd, 414, 221, "inbound/test.bufr"));
	ensure_throws(v.validate(fd, 634, 0, "inbound/test.bufr"));
	ensure_throws(v.validate(fd, 634, 10, "inbound/test.bufr"));

	close(fd);

	metadata::Collection mdc;
	scan::scan("inbound/test.bufr", mdc);
	buf = mdc[0].getData();

	v.validate(buf.data(), buf.size());
	ensure_throws(v.validate((const char*)buf.data()+1, buf.size()-1));
	ensure_throws(v.validate(buf.data(), buf.size()-1));
}

// Test scanning a BUFR file that can only be decoded partially
template<> template<>
void to::test<4>()
{
	Metadata md;
	scan::Bufr scanner;
	types::Time reftime;
	wibble::sys::Buffer buf;

	scanner.open("inbound/C23000.bufr");

	// See how we scan the first BUFR
	ensure(scanner.next(md));

	// Check the source info
	ensure_equals(md.source, Item<Source>(source::Blob::create("bufr", sys::fs::abspath("inbound/C23000.bufr"), 0, 2206)));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 2206u);
	ensure_equals(string((const char*)buf.data(), 4), "BUFR");
	ensure_equals(string((const char*)buf.data() + 2202, 4), "7777");

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::BUFR::create(98, 0)));

	// Check product
	ValueBag vb;
	vb.set("t", Value::createString("temp"));
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::BUFR::create(2, 255, 101, vb)));

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2010, 7, 21, 23, 0, 0))));

	// Check area
	ensure(md.has(types::TYPE_AREA));

	// Check run
	ensure(not md.has(types::TYPE_RUN));


	// No more bufrs
	ensure(not scanner.next(md));
}

// Test scanning a pollution BUFR file
template<> template<>
void to::test<5>()
{
	Metadata md;
	scan::Bufr scanner;
	types::Time reftime;
	wibble::sys::Buffer buf;

	scanner.open("inbound/pollution.bufr");

	// See how we scan the first BUFR
	ensure(scanner.next(md));

	// Check the source info
	ensure_equals(md.source, Item<Source>(source::Blob::create("bufr", sys::fs::abspath("inbound/pollution.bufr"), 0, 178)));

	// Check that the source can be read properly
	buf = md.getData();
	ensure_equals(buf.size(), 178u);
	ensure_equals(string((const char*)buf.data(), 4), "BUFR");
	ensure_equals(string((const char*)buf.data() + 174, 4), "7777");

	// Check origin
	ensure(md.get(types::TYPE_ORIGIN).defined());
	ensure_equals(md.get(types::TYPE_ORIGIN), Item<>(origin::BUFR::create(98, 0)));

	// Check product
	ValueBag vb;
	vb.set("t", Value::createString("pollution"));
	vb.set("p", Value::createString("NO2"));
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::BUFR::create(8, 255, 171, vb)));

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2010, 8, 8, 23, 0, 0))));

	// Check area
	ensure(md.has(types::TYPE_AREA));
	ensure_equals(md.get<Area>(), Area::decodeString("GRIB(gems=IT0002, lat=4601194, lon=826889, name=NO_3118_PIEVEVERGONTE)"));

	// Check run
	ensure(not md.has(types::TYPE_RUN));


	// No more bufrs
	ensure(not scanner.next(md));
}

}

// vim:set ts=4 sw=4:
