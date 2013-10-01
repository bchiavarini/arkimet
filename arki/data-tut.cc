/*
 * Copyright (C) 2012--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "config.h"

#include <arki/tests/tests.h>
#include "data.h"
#include "data/impl.h"
#include "data/lines.h"

namespace tut {
using namespace std;
using namespace arki;
using namespace wibble;
using namespace wibble::tests;

struct arki_data_shar {
};

TESTGRP(arki_data);

template<> template<>
void to::test<1>()
{
    using namespace arki::data;

    Writer w1 = Writer::get("grib", "test-data-writer");
    Writer w2 = Writer::get("grib", "test-data-writer");

    // Check that the implementation is reused
    wassert(actual(w1._implementation()) == w2._implementation());
}

}
