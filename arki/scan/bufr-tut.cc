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
#include <arki/types/run.h>
#include <arki/metadata.h>
#include <wibble/sys/fs.h>
#include <dballe++/init.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;

struct arki_scan_bufr_shar {
	dballe::DballeInit dballeInit;
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
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::BUFR::create(0, 255, 1)));

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2005, 12, 1, 18, 0, 0))));

	// Check run
	ensure_equals(md.get(types::TYPE_RUN).upcast<Run>()->style(), Run::MINUTE);
	ensure_equals(md.get(types::TYPE_RUN), Item<>(run::Minute::create(18)));


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
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::BUFR::create(0, 255, 1)));

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2004, 11, 30, 12, 0, 0))));

	// Check run
	ensure_equals(md.get(types::TYPE_RUN).upcast<Run>()->style(), Run::MINUTE);
	ensure_equals(md.get(types::TYPE_RUN), Item<>(run::Minute::create(12)));


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
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::BUFR::create(0, 255, 3)));

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2004, 11, 30, 12, 0, 0))));

	// Check run
	ensure_equals(md.get(types::TYPE_RUN).upcast<Run>()->style(), Run::MINUTE);
	ensure_equals(md.get(types::TYPE_RUN), Item<>(run::Minute::create(12)));
	

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
	ensure(md.get(types::TYPE_PRODUCT).defined());
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::BUFR::create(0, 255, 1)));

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2005, 12, 1, 18, 0, 0))));

	// Check run
	ensure_equals(md.get(types::TYPE_RUN).upcast<Run>()->style(), Run::MINUTE);
	ensure_equals(md.get(types::TYPE_RUN), Item<>(run::Minute::create(18)));


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
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::BUFR::create(0, 255, 1)));

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2004, 11, 30, 12, 0, 0))));

	// Check run
	ensure_equals(md.get(types::TYPE_RUN).upcast<Run>()->style(), Run::MINUTE);
	ensure_equals(md.get(types::TYPE_RUN), Item<>(run::Minute::create(12)));


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
	ensure_equals(md.get(types::TYPE_PRODUCT), Item<>(product::BUFR::create(0, 255, 3)));

	// Check reftime
	ensure_equals(md.get(types::TYPE_REFTIME).upcast<Reftime>()->style(), Reftime::POSITION);
	ensure_equals(md.get(types::TYPE_REFTIME), Item<>(reftime::Position::create(types::Time::create(2004, 11, 30, 12, 0, 0))));

	// Check run
	ensure_equals(md.get(types::TYPE_RUN).upcast<Run>()->style(), Run::MINUTE);
	ensure_equals(md.get(types::TYPE_RUN), Item<>(run::Minute::create(12)));
	

	// No more bufrs
	ensure(not scanner.next(md));
}

}

// vim:set ts=4 sw=4:
