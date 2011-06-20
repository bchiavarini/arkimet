/*
 * Copyright (C) 2007--2011  Enrico Zini <enrico@enricozini.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */

#include <arki/matcher/test-utils.h>
#include <arki/matcher.h>
#include <arki/matcher/timerange.h>
#include <arki/metadata.h>
#include <arki/types/timerange.h>

#include <memory>
#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;

struct arki_matcher_timerange_shar {
	Metadata md;

	arki_matcher_timerange_shar()
	{
		md.create();
		arki::tests::fill(md);
	}
};
TESTGRP(arki_matcher_timerange);

// Try matching GRIB1 timerange
template<> template<>
void to::test<1>()
{
	ensure_matches("timerange:GRIB1", md);
	ensure_matches("timerange:GRIB1,,", md);
	ensure_matches("timerange:GRIB1,2", md);
	ensure_matches("timerange:GRIB1,2,,", md);
	// Incomplete matches are not allowed
	ensure_not_matches("timerange:GRIB1,,22s,", md);
	ensure_not_matches("timerange:GRIB1,,,23s", md);
	// ensure_not_matches("timerange:GRIB1,2,22s,", md);
	ensure_not_matches("timerange:GRIB1,2,,23s", md);
	ensure_not_matches("timerange:GRIB1,,22s,23s", md);
	ensure_matches("timerange:GRIB1,2,22s,23s", md);
	ensure_not_matches("timerange:GRIB1,2,23s,23s", md);
	ensure_not_matches("timerange:GRIB1,2,22s,22s", md);
	// ensure_not_matches("timerange:GRIB1,2,22,23");

	// Match timerange in years, which gave problems
	md.set(timerange::GRIB1::create(2, 4, 2, 3));
	ensure_matches("timerange:GRIB1,2,2y,3y", md);
}

// Try matching GRIB2 timerange
template<> template<>
void to::test<2>()
{
	md.set(timerange::GRIB2::create(1, 2, 3, 4));

	ensure_matches("timerange:GRIB2", md);
	ensure_matches("timerange:GRIB2,,", md);
	ensure_matches("timerange:GRIB2,1", md);
	ensure_matches("timerange:GRIB2,1,,", md);
	ensure_matches("timerange:GRIB2,1,2,", md);
	ensure_matches("timerange:GRIB2,1,2,3", md);
	ensure_matches("timerange:GRIB2,1,2,3,4", md);
	// Incomplete matches are allowed
	ensure_matches("timerange:GRIB2,,2,", md);
	ensure_matches("timerange:GRIB2,,,3", md);
	ensure_matches("timerange:GRIB2,,,,4", md);
	ensure_not_matches("timerange:GRIB1", md);
	ensure_not_matches("timerange:GRIB2,2", md);
	ensure_not_matches("timerange:GRIB2,,3", md);
	ensure_not_matches("timerange:GRIB2,,,4", md);
	ensure_not_matches("timerange:GRIB2,,,,5", md);
}

// Try matching Timedef timerange
template<> template<>
void to::test<3>()
{
    md.set(timerange::Timedef::createFromYaml("6h,1,3h"));

    ensure_matches("timerange:Timedef", md);
    ensure_matches("timerange:Timedef,,", md);
    ensure_matches("timerange:Timedef,6h", md);
    ensure_matches("timerange:Timedef,6h,,", md);
    ensure_matches("timerange:Timedef,6h,1,", md);
    ensure_matches("timerange:Timedef,6h,1,3h", md);
    // Incomplete matches are allowed
    ensure_matches("timerange:Timedef,,1,", md);
    ensure_matches("timerange:Timedef,,,3h", md);
    ensure_not_matches("timerange:GRIB1", md);
    ensure_not_matches("timerange:Timedef,5h", md);
    ensure_not_matches("timerange:Timedef,6h,2", md);
    ensure_not_matches("timerange:Timedef,6h,1,4h", md);
    ensure_not_matches("timerange:Timedef,,2", md);
    ensure_not_matches("timerange:Timedef,,2,4h", md);
    ensure_not_matches("timerange:Timedef,,,4h", md);
    ensure_not_matches("timerange:Timedef,-", md);
    ensure_not_matches("timerange:Timedef,6h,-", md);
    ensure_not_matches("timerange:Timedef,6h,2,-", md);

    md.set(timerange::Timedef::createFromYaml("6h"));
    ensure_matches("timerange:Timedef", md);
    ensure_matches("timerange:Timedef,,", md);
    ensure_matches("timerange:Timedef,6h", md);
    ensure_matches("timerange:Timedef,6h,,", md);
    ensure_matches("timerange:Timedef,6h,-", md);
    ensure_matches("timerange:Timedef,6h,-,-", md);
    ensure_not_matches("timerange:GRIB1", md);
    ensure_not_matches("timerange:Timedef,5h", md);
    ensure_not_matches("timerange:Timedef,6h,1", md);
    ensure_not_matches("timerange:Timedef,6h,1,4h", md);
    ensure_not_matches("timerange:Timedef,,2", md);
    ensure_not_matches("timerange:Timedef,,2,4h", md);
    ensure_not_matches("timerange:Timedef,,,4h", md);
    ensure_not_matches("timerange:Timedef,-", md);
    ensure_not_matches("timerange:Timedef,6h,2,-", md);

    md.set(timerange::Timedef::createFromYaml("6h,2"));
    ensure_matches("timerange:Timedef", md);
    ensure_matches("timerange:Timedef,,", md);
    ensure_matches("timerange:Timedef,6h", md);
    ensure_matches("timerange:Timedef,6h,,", md);
    ensure_matches("timerange:Timedef,6h,,-", md);
    ensure_matches("timerange:Timedef,6h,2,-", md);
    ensure_not_matches("timerange:GRIB1", md);
    ensure_not_matches("timerange:Timedef,5h", md);
    ensure_not_matches("timerange:Timedef,6h,1", md);
    ensure_not_matches("timerange:Timedef,6h,1,-", md);
    ensure_not_matches("timerange:Timedef,6h,2,4h", md);
    ensure_not_matches("timerange:Timedef,,2,4h", md);
    ensure_not_matches("timerange:Timedef,,,4h", md);
    ensure_not_matches("timerange:Timedef,-", md);
    ensure_not_matches("timerange:Timedef,6h,-", md);
}

// Try matching BUFR timerange
template<> template<>
void to::test<4>()
{
	md.set(timerange::BUFR::create(2, 1));

	ensure_matches("timerange:BUFR", md);
	ensure_matches("timerange:BUFR,2h", md);
	ensure_matches("timerange:BUFR,120m", md);
	ensure_matches("timerange:BUFR,7200s", md);
	ensure_not_matches("timerange:GRIB1", md);
	ensure_not_matches("timerange:GRIB2", md);
	ensure_not_matches("timerange:BUFR,3h", md);
	ensure_not_matches("timerange:BUFR,2m", md);

	md.set(timerange::BUFR::create());

	ensure_matches("timerange:BUFR", md);
	ensure_matches("timerange:BUFR,0", md);
	ensure_matches("timerange:BUFR,0h", md);
	ensure_not_matches("timerange:GRIB1", md);
	ensure_not_matches("timerange:GRIB2", md);
	ensure_not_matches("timerange:BUFR,3h", md);
	ensure_not_matches("timerange:BUFR,2m", md);
}


// Try some cases that have been buggy
template<> template<>
void to::test<5>()
{
	Metadata md;

	md.set(timerange::GRIB1::create(0, 254, 0, 0));
	ensure_matches("timerange:GRIB1,0,0h", md);
	ensure_matches("timerange:GRIB1,0,0h,0h", md);
	ensure_not_matches("timerange:GRIB1,0,12h", md);
	ensure_not_matches("timerange:GRIB1,0,12y", md);
	ensure_not_matches("timerange:GRIB1,0,,12y", md);
}

// Test timedef matcher parsing
template<> template<>
void to::test<6>()
{
    {
        auto_ptr<matcher::MatchTimerange> matcher(matcher::MatchTimerange::parse("Timedef,+72h,1,6h"));
        const matcher::MatchTimerangeTimedef* m = dynamic_cast<const matcher::MatchTimerangeTimedef*>(matcher.get());
        ensure(m);

        ensure(m->has_step);
        ensure_equals(m->step, 72 * 3600);
        ensure(m->step_is_seconds);

        ensure(m->has_proc_type);
        ensure_equals(m->proc_type, 1);

        ensure(m->has_proc_duration);
        ensure_equals(m->proc_duration, 6 * 3600);
        ensure(m->proc_duration_is_seconds);
    }

    {
        auto_ptr<matcher::MatchTimerange> matcher(matcher::MatchTimerange::parse("Timedef,72h"));
        const matcher::MatchTimerangeTimedef* m = dynamic_cast<const matcher::MatchTimerangeTimedef*>(matcher.get());
        ensure(m);

        ensure(m->has_step);
        ensure_equals(m->step, 72 * 3600);
        ensure(m->step_is_seconds);

        ensure(not m->has_proc_type);
        ensure(not m->has_proc_duration);
    }

    {
        auto_ptr<matcher::MatchTimerange> matcher(matcher::MatchTimerange::parse("Timedef,,-"));
        const matcher::MatchTimerangeTimedef* m = dynamic_cast<const matcher::MatchTimerangeTimedef*>(matcher.get());
        ensure(m);

        ensure(not m->has_step);

        ensure(m->has_proc_type);
        ensure_equals(m->proc_type, -1);
    }
}

// Try matching timedef timerange on GRIB1
template<> template<>
void to::test<7>()
{
    md.set(timerange::GRIB1::create(0, 0, 60, 0));
    ensure_matches("timerange:Timedef,1h", md);
    ensure_matches("timerange:Timedef,1h,254", md);
    ensure_matches("timerange:Timedef,60m,254,0", md);
    ensure_not_matches("timerange:Timedef,2h", md);
    ensure_not_matches("timerange:Timedef,1h,1,0", md);
    ensure_not_matches("timerange:Timedef,1h,254,1s", md);

    md.set(timerange::GRIB1::create(3, 1, 1, 3));
    ensure_matches("timerange:Timedef,+3h", md);
    ensure_matches("timerange:Timedef,+3h,0", md);
    ensure_matches("timerange:Timedef,+3h,0,2h", md);
    ensure_not_matches("timerange:Timedef,+2h", md);
    ensure_not_matches("timerange:Timedef,+3h,1,2h", md);
    ensure_not_matches("timerange:Timedef,+3h,0,1h", md);
}

// Try matching timedef timerange on GRIB2
template<> template<>
void to::test<8>()
{
    // On GRIB2
    md.set(timerange::GRIB2::create(3, 1, 1, 3));
    // timerange:GRIB2 metadata items do not carry enough information to feed Timedef matchers
    ensure_matches("timerange:Timedef", md);
    ensure_matches("timerange:Timedef,-", md);
    ensure_matches("timerange:Timedef,-,-", md);
    ensure_matches("timerange:Timedef,-,-,-", md);
    ensure_not_matches("timerange:Timedef,+3h", md);
    ensure_not_matches("timerange:Timedef,+3h,0", md);
    ensure_not_matches("timerange:Timedef,+3h,0,2h", md);
}

// Try matching timedef timerange on Timedef
template<> template<>
void to::test<9>()
{
    md.set(timerange::Timedef::createFromYaml("72h,1,6h"));
    ensure_matches("timerange:Timedef,+72h", md);
    ensure_matches("timerange:Timedef,+72h,1", md);
    ensure_matches("timerange:Timedef,+72h,1,6h", md);
    ensure_matches("timerange:Timedef,+72h,1", md);
    ensure_matches("timerange:Timedef,+72h,,6h", md);
    ensure_matches("timerange:Timedef,,1", md);
    ensure_matches("timerange:Timedef,,1,6h", md);
    ensure_matches("timerange:Timedef,,,6h", md);
    ensure_not_matches("timerange:Timedef,73h", md);
    ensure_not_matches("timerange:Timedef,72h,-", md);
    ensure_not_matches("timerange:Timedef,72h,-", md);

    md.set(timerange::Timedef::createFromYaml("72h"));
    ensure_matches("timerange:Timedef,+72h", md);
    ensure_matches("timerange:Timedef,+72h,-", md);
    ensure_matches("timerange:Timedef,+72h,-,-", md);
    ensure_matches("timerange:Timedef,+72h,,-", md);
    ensure_matches("timerange:Timedef,,-", md);
    ensure_matches("timerange:Timedef,,-,-", md);
    ensure_matches("timerange:Timedef,,,-", md);
    ensure_not_matches("timerange:Timedef,73h", md);
    ensure_not_matches("timerange:Timedef,72h,1", md);

    md.set(timerange::Timedef::createFromYaml("72h,1"));
    ensure_matches("timerange:Timedef,+72h", md);
    ensure_matches("timerange:Timedef,+72h,1", md);
    ensure_matches("timerange:Timedef,+72h,1,-", md);
    ensure_matches("timerange:Timedef,+72h,,-", md);
    ensure_matches("timerange:Timedef,,1", md);
    ensure_matches("timerange:Timedef,,1,-", md);
    ensure_matches("timerange:Timedef,,,-", md);
    ensure_not_matches("timerange:Timedef,73h", md);
    ensure_not_matches("timerange:Timedef,72h,2", md);
    ensure_not_matches("timerange:Timedef,72h,1,3h", md);

    md.set(timerange::Timedef::createFromYaml("72d,1,6h"));
    ensure_matches("timerange:Timedef,+72d", md);
    ensure_matches("timerange:Timedef,+72d,1", md);
    ensure_matches("timerange:Timedef,+72d,1,6h", md);
    ensure_matches("timerange:Timedef,+72d,1", md);
    ensure_matches("timerange:Timedef,+72d,,6h", md);
    ensure_matches("timerange:Timedef,,1", md);
    ensure_matches("timerange:Timedef,,1,6h", md);
    ensure_matches("timerange:Timedef,,,6h", md);
    ensure_not_matches("timerange:Timedef,73d", md);
    ensure_not_matches("timerange:Timedef,72d,-", md);
    ensure_not_matches("timerange:Timedef,72d,-", md);
}

// Try matching timedef timerange on BUFR
template<> template<>
void to::test<10>()
{
    md.set(timerange::BUFR::create(2, 1));
    ensure_matches("timerange:Timedef,+2h", md);
    ensure_matches("timerange:Timedef,+2h,-", md);
    ensure_matches("timerange:Timedef,+2h,-,-", md);
    ensure_not_matches("timerange:Timedef,+1h", md);
    ensure_not_matches("timerange:Timedef,-", md);
    ensure_not_matches("timerange:Timedef,2h,1", md);
}

// Try matching timedef timerange on known values
template<> template<>
void to::test<11>()
{
    md.set(Timerange::decodeString("Timedef(3h)"));
    ensure_matches("timerange:Timedef,+3h,-", md);
    ensure_not_matches("timerange:Timedef,+3h, 254", md);

    md.set(Timerange::decodeString("Timedef(3h)"));
    ensure_matches("timerange:Timedef,+3h,-", md);
    ensure_not_matches("timerange:Timedef,+3h, 254", md);

    md.set(Timerange::decodeString("Timedef(3h,254)"));
    ensure_matches("timerange:Timedef,+3h, 254", md);

    md.set(Timerange::decodeString("GRIB1(000, 003h)"));
    ensure_matches("timerange:Timedef,+3h, 254", md);

    md.set(Timerange::decodeString("Timedef(0s,254)"));
    ensure_matches("timerange:Timedef,0,254", md);

    md.set(Timerange::decodeString("GRIB1(000, 000h)"));
    ensure_matches("timerange:Timedef,0,254", md);

    /////
    md.set(Timerange::decodeString("GRIB1(004, 000h, 003h)"));
    ensure_not_matches("timerange:Timedef,3h,,1h", md);

    md.set(Timerange::decodeString("GRIB1(000, 000h)"));
    ensure_not_matches("timerange:Timedef,3h,,1h", md);

    md.set(Timerange::decodeString("GRIB1(004, 000h, 012h)"));
    ensure_not_matches("timerange:Timedef,,1,3h", md);

    md.set(Timerange::decodeString("GRIB1(013, 000h, 012h)"));
    ensure_not_matches("timerange:Timedef,,1,3h", md);

    md.set(Timerange::decodeString("GRIB1(000, 000h)"));
    ensure_not_matches("timerange:Timedef,,1,3h", md);

    md.set(Timerange::decodeString("GRIB1(013, 000h, 012h)"));
    ensure_matches("timerange:GRIB1,13,0", md);
}

// Test some serialisation scenarios
template<> template<>
void to::test<12>()
{
    Matcher m = Matcher::parse("timerange:Timedef,,1,3h");
    //Matcher m1 = Matcher::parse(m.toStringExpanded());
    //ensure_equals(m, m1);
    ensure_equals(m.toStringExpanded(), "timerange:Timedef,,1,3h");
}

}

// vim:set ts=4 sw=4:
