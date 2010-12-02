/*
 * Copyright (C) 2007,2008,2009  Enrico Zini <enrico@enricozini.org>
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

#include <arki/dataset/test-utils.h>
#include <arki/dataset/ondisk2.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/matcher.h>
#include <arki/summary.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <wibble/sys/fs.h>
#include <wibble/stream/posix.h>

#include <unistd.h>
#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;
using namespace arki::dataset;
using namespace arki::dataset::ondisk2;
using namespace arki::utils;
using namespace arki::utils::files;

struct arki_dataset_ondisk2_reader_shar : public arki::tests::DatasetTest {
	arki_dataset_ondisk2_reader_shar()
	{
		cfg.setValue("path", "testds");
		cfg.setValue("name", "testds");
		cfg.setValue("type", "ondisk2");
		cfg.setValue("step", "daily");
		cfg.setValue("postprocess", "testcountbytes");
		
		clean_and_import();
	}
};
TESTGRP(arki_dataset_ondisk2_reader);

// Test querying with postprocessing
template<> template<>
void to::test<1>()
{
	auto_ptr<ondisk2::Reader> reader(makeOndisk2Reader());
	ensure(reader->hasWorkingIndex());

	// Use dup() because PosixBuf will close its file descriptor at destruction
	// time
	stream::PosixBuf pb(dup(2));
	ostream os(&pb);
	dataset::ByteQuery bq;
	bq.setPostprocess(Matcher::parse("origin:GRIB1,200"), "testcountbytes");
	reader->queryBytes(bq, os);

	string out = sys::fs::readFile("testcountbytes.out");
	ensure_equals(out, "7400\n");
}

// Test that summary files are not created for all the extent of the query, but
// only for data present in the dataset
template<> template<>
void to::test<2>()
{
	auto_ptr<ondisk2::Reader> reader(makeOndisk2Reader());

	Summary s;
	reader->querySummary(Matcher::parse("reftime:=2007"), s);
	ensure_equals(s.count(), 3u);
	ensure_equals(s.size(), 44412u);

	// Global summary is not built because we only query specific months
	ensure(!sys::fs::access("testds/.summaries/all.summary", F_OK));

	ensure(!sys::fs::access("testds/.summaries/2007-01.summary", F_OK));
	ensure(!sys::fs::access("testds/.summaries/2007-02.summary", F_OK));
	ensure(!sys::fs::access("testds/.summaries/2007-03.summary", F_OK));
	ensure(!sys::fs::access("testds/.summaries/2007-04.summary", F_OK));
	ensure(!sys::fs::access("testds/.summaries/2007-05.summary", F_OK));
	ensure(!sys::fs::access("testds/.summaries/2007-06.summary", F_OK));
	ensure(sys::fs::access("testds/.summaries/2007-07.summary", F_OK));
	// Summary caches corresponding to DB holes are still created and used
	ensure(sys::fs::access("testds/.summaries/2007-08.summary", F_OK));
	ensure(sys::fs::access("testds/.summaries/2007-09.summary", F_OK));
	ensure(sys::fs::access("testds/.summaries/2007-10.summary", F_OK));
	ensure(!sys::fs::access("testds/.summaries/2007-11.summary", F_OK));
	ensure(!sys::fs::access("testds/.summaries/2007-12.summary", F_OK));
	ensure(!sys::fs::access("testds/.summaries/2008-01.summary", F_OK));
}

}

// vim:set ts=4 sw=4:
