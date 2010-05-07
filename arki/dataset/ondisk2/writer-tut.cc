/*
 * Copyright (C) 2007--2010  Enrico Zini <enrico@enricozini.org>
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

#include <arki/dataset/ondisk2/test-utils.h>
#include <arki/dataset/ondisk2/writer.h>
#include <arki/dataset/ondisk2/reader.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/configfile.h>
#include <arki/scan/grib.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <wibble/sys/fs.h>

#include <sstream>
#include <iostream>
#include <algorithm>

using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;
using namespace arki::dataset::ondisk2;
using namespace arki::dataset::ondisk2::writer;
using namespace arki::utils::files;

namespace tut {

struct arki_dataset_ondisk2_writer_shar : public dataset::maintenance::MaintFileVisitor {
	// Little dirty hack: implement MaintFileVisitor so we can conveniently
	// access State

	ConfigFile cfg;

	arki_dataset_ondisk2_writer_shar()
	{
		system("rm -rf testdir");

		cfg.setValue("path", "testdir");
		cfg.setValue("name", "testdir");
		cfg.setValue("type", "ondisk2");
		cfg.setValue("step", "daily");
		cfg.setValue("unique", "origin, reftime");
	}

	void acquireSamples()
	{
		Metadata md;
		scan::Grib scanner;
		Writer writer(cfg);
		scanner.open("inbound/test.grib1");
		size_t count = 0;
		for ( ; scanner.next(md); ++count)
			ensure_equals(writer.acquire(md), WritableDataset::ACQ_OK);
		ensure_equals(count, 3u);
		writer.flush();
	}

	virtual void operator()(const std::string& file, State state) {}
};
TESTGRP(arki_dataset_ondisk2_writer);

// Test accuracy of maintenance scan, on dataset with one file deleted,
// performing repack
template<> template<>
void to::test<1>()
{
	acquireSamples();
	removeDontpackFlagfile("testdir");

	system("rm testdir/2007/07-07.grib1");

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 3u);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.count(DELETED), 1u);
	ensure_equals(c.remaining(), string());
	ensure(not c.isClean());

	{
		// Test packing has something to report
		stringstream s;
		writer.repack(s, false);
		ensure_equals(s.str(),
			"testdir: 2007/07-07.grib1 should be removed from the index\n"
			"testdir: 1 file should be removed from the index.\n");

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(OK), 2u);
		ensure_equals(c.count(DELETED), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	{
		// Perform packing and check that things are still ok afterwards
		stringstream s;
		writer.repack(s, true);
		ensure_equals(s.str(),
			"testdir: deleted from index 2007/07-07.grib1\n"
			"testdir: 1 file removed from index, 30448 bytes reclaimed on the index, 30448 total bytes freed.\n");
		c.clear();

		writer.maintenance(c);
		ensure_equals(c.count(OK), 2u);
		ensure_equals(c.remaining(), string());
		ensure(c.isClean());
	}

	// Perform full maintenance and check that things are still ok afterwards
	{
		stringstream s;
		writer.check(s, true, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
		c.clear();
		writer.maintenance(c);
		ensure_equals(c.count(OK), 2u);
		ensure_equals(c.remaining(), string());
		ensure(c.isClean());
	}

	// Ensure that we have the summary cache
	ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
}

// Test accuracy of maintenance scan, on dataset with one file deleted,
// performing check
template<> template<>
void to::test<2>()
{
	acquireSamples();

	system("rm testdir/2007/07-07.grib1");

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 3u);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.count(DELETED), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	stringstream s;

	// Perform full maintenance and check that things are still ok afterwards
	writer.check(s, true, true);
	ensure_equals(s.str(),
		"testdir: deindexed 2007/07-07.grib1\n"
		"testdir: 1 file removed from index, 30448 bytes reclaimed cleaning the index.\n");

	c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Perform packing and check that things are still ok afterwards
	s.str(std::string());
	writer.repack(s, true);
	ensure_equals(s.str(), string()); // Nothing should have happened
	c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Ensure that we have the summary cache
	ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
}

// Test accuracy of maintenance scan, on dataset with one file to reclaim,
// performing repack
template<> template<>
void to::test<3>()
{
	acquireSamples();
	removeDontpackFlagfile("testdir");
	{
		// Remove one element
		arki::dataset::ondisk2::Writer writer(cfg);
		writer.remove("2");
		writer.flush();
	}

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 3u);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.count(TO_INDEX), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	{
		// Test packing has something to report
		stringstream s;
		writer.repack(s, false);
		ensure_equals(s.str(),
			"testdir: 2007/07-07.grib1 should be deleted\n"
			"testdir: 1 file should be deleted.\n");

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(OK), 2u);
		ensure_equals(c.count(TO_INDEX), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	// Perform packing and check that things are still ok afterwards
	{
		stringstream s;
		writer.repack(s, true);
		ensure_equals(s.str(),
			"testdir: deleted 2007/07-07.grib1 (34960 freed)\n"
			"testdir: 1 file deleted, 30448 bytes reclaimed on the index, 65408 total bytes freed.\n");

		c.clear();

		writer.maintenance(c);
		ensure_equals(c.count(OK), 2u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}

	// Perform full maintenance and check that things are still ok afterwards
	{
		stringstream s;
		writer.check(s, true, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
		c.clear();
		writer.maintenance(c);
		ensure_equals(c.count(OK), 2u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}

	// Ensure that we have the summary cache
	ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
}

// Test accuracy of maintenance scan, on dataset with one file to reclaim,
// performing check
template<> template<>
void to::test<4>()
{
	acquireSamples();
	{
		// Remove one element
		arki::dataset::ondisk2::Writer writer(cfg);
		writer.remove("2");
		writer.flush();
	}

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 3u);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.count(TO_INDEX), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	stringstream s;

	// Perform full maintenance and check that things are still ok afterwards
	writer.check(s, true, true);
	ensure_equals(s.str(),
		"testdir: rescanned 2007/07-07.grib1\n"
		"testdir: 1 file rescanned, 30448 bytes reclaimed cleaning the index.\n");
	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(OK), 3u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Perform packing and check that things are still ok afterwards
	s.str(std::string());
	writer.repack(s, true);
	ensure_equals(s.str(), string()); // Nothing should have happened
	c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(OK), 3u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Ensure that we have the summary cache
	ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
}

// Test accuracy of maintenance scan, on dataset with one file to pack,
// performing repack
template<> template<>
void to::test<5>()
{
	cfg.setValue("step", "monthly");
	acquireSamples();
	removeDontpackFlagfile("testdir");
	{
		// Remove one element
		arki::dataset::ondisk2::Writer writer(cfg);
		writer.remove("2");
		writer.flush();
	}

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 2u);
	ensure_equals(c.count(OK), 1u);
	ensure_equals(c.count(TO_PACK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	// Perform packing and check that things are still ok afterwards
	stringstream s;
	writer.repack(s, true);
	ensure_equals(s.str(),
		"testdir: packed 2007/07.grib1 (34960 saved)\n"
		"testdir: 1 file packed, 30448 bytes reclaimed on the index, 65408 total bytes freed.\n");
	c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Perform full maintenance and check that things are still ok afterwards
	s.str(std::string());
	writer.check(s, true, true);
	ensure_equals(s.str(), string()); // Nothing should have happened
	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Ensure that we have the summary cache
	ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
}

// Test accuracy of maintenance scan, on dataset with one file to pack,
// performing check
template<> template<>
void to::test<6>()
{
	cfg.setValue("step", "monthly");
	acquireSamples();
	{
		// Remove one element
		arki::dataset::ondisk2::Writer writer(cfg);
		writer.remove("2");
		writer.flush();
	}

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 2u);
	ensure_equals(c.count(OK), 1u);
	ensure_equals(c.count(TO_PACK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	stringstream s;

	// Perform full maintenance and check that things are still ok afterwards
	// No packing occurs here: check does not mangle data files
	writer.check(s, true, true);
	ensure_equals(s.str(),
		"testdir: 30448 bytes reclaimed cleaning the index.\n");

	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(OK), 1u);
	ensure_equals(c.count(TO_PACK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	// Perform packing and check that things are still ok afterwards
	s.str(std::string());
	writer.repack(s, true);
	ensure_equals(s.str(),
		"testdir: packed 2007/07.grib1 (34960 saved)\n"
		"testdir: 1 file packed, 2576 bytes reclaimed on the index, 37536 total bytes freed.\n");
	c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Ensure that we have the summary cache
	ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
}

// Test accuracy of maintenance scan, after deleting the index
template<> template<>
void to::test<7>()
{
	acquireSamples();
	system("rm testdir/index.sqlite");

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 3u);
	ensure_equals(c.count(OK), 0u);
	ensure_equals(c.count(TO_INDEX), 3u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	stringstream s;

	// Perform full maintenance and check that things are still ok afterwards
	writer.check(s, true, true);
	ensure_equals(s.str(),
		"testdir: rescanned 2007/07-07.grib1\n"
		"testdir: rescanned 2007/07-08.grib1\n"
		"testdir: rescanned 2007/10-09.grib1\n"
		"testdir: 3 files rescanned, 30448 bytes reclaimed cleaning the index.\n");
	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(OK), 3u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Perform packing and check that things are still ok afterwards
	s.str(std::string());
	writer.repack(s, true);
	ensure_equals(s.str(), string()); // Nothing should have happened
	c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(OK), 3u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Ensure that we have the summary cache
	ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
}

// Test accuracy of maintenance scan, after deleting the index, with some
// spurious extra files in the dataset
template<> template<>
void to::test<8>()
{
	acquireSamples();
	system("rm testdir/index.sqlite");
	system("echo 'GRIB garbage 7777' > testdir/2007/07.grib1.tmp");

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 3u);
	ensure_equals(c.count(TO_INDEX), 3u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	stringstream s;

	// Perform full maintenance and check that things are still ok afterwards
	writer.check(s, true, true);
	ensure_equals(s.str(),
		"testdir: rescanned 2007/07-07.grib1\n"
		"testdir: rescanned 2007/07-08.grib1\n"
		"testdir: rescanned 2007/10-09.grib1\n"
		"testdir: 3 files rescanned, 30448 bytes reclaimed cleaning the index.\n");
	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(OK), 3u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	ensure(sys::fs::access("testdir/2007/07.grib1.tmp", F_OK));

	// Perform packing and check that things are still ok afterwards
	s.str(std::string());
	writer.repack(s, true);
	ensure_equals(s.str(), string()); // Nothing should have happened
	c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(OK), 3u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Ensure that we have the summary cache
	ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
}

// Test recreating a dataset from random datafiles
template<> template<>
void to::test<9>()
{
	system("mkdir testdir");
	system("mkdir testdir/foo");
	system("mkdir testdir/foo/bar");
	system("cp inbound/test.grib1 testdir/foo/bar/");
	system("echo 'GRIB garbage 7777' > testdir/foo/bar/test.grib1.tmp");

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 1u);
	ensure_equals(c.count(TO_INDEX), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	stringstream s;

	// Perform full maintenance and check that things are still ok afterwards
	writer.check(s, true, true);
	ensure_equals(s.str(),
		"testdir: rescanned foo/bar/test.grib1\n"
		"testdir: 1 file rescanned, 30448 bytes reclaimed cleaning the index.\n");
	c.clear();
	writer.maintenance(c);
	// A repack is still needed because the data is not sorted by reftime
	ensure_equals(c.fileStates.size(), 1u);
	ensure_equals(c.count(TO_PACK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	ensure(sys::fs::access("testdir/foo/bar/test.grib1.tmp", F_OK));
	ensure_equals(utils::files::size("testdir/foo/bar/test.grib1"), 44412);

	// Perform packing and check that things are still ok afterwards
	s.str(std::string());
	writer.repack(s, true);
	ensure_equals(s.str(),
		"testdir: packed foo/bar/test.grib1 (0 saved)\n"
		"testdir: 1 file packed, 2576 bytes reclaimed on the index, 2576 total bytes freed.\n");
	c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(OK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	ensure_equals(utils::files::size("testdir/foo/bar/test.grib1"), 44412);

	// Test querying
	Reader reader(cfg);
	ensure(reader.hasWorkingIndex());
	metadata::Collection mdc;
	reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
	ensure_equals(mdc.size(), 1u);
	UItem<source::Blob> blob = mdc[0].source.upcast<source::Blob>();
	ensure_equals(blob->format, "grib1"); 
	ensure_equals(blob->filename, sys::fs::abspath("testdir/foo/bar/test.grib1"));
	ensure_equals(blob->offset, 34960u);

	// Ensure that we have the summary cache
	ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
}

// Test recreating a dataset from just a datafile with duplicate data and a rebuild flagfile
template<> template<>
void to::test<10>()
{
	system("mkdir testdir");
	system("mkdir testdir/foo");
	system("mkdir testdir/foo/bar");
	system("cat inbound/test.grib1 inbound/test.grib1 > testdir/foo/bar/test.grib1");

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 1u);
	ensure_equals(c.count(TO_INDEX), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	stringstream s;

	// Perform full maintenance and check that things are still ok afterwards
	writer.check(s, true, true);
	ensure_equals(s.str(),
		"testdir: rescanned foo/bar/test.grib1\n"
		"testdir: 1 file rescanned, 30448 bytes reclaimed cleaning the index.\n");

	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(TO_PACK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	ensure_equals(utils::files::size("testdir/foo/bar/test.grib1"), 44412*2);

	{
		// Test querying: reindexing should have chosen the last version of
		// duplicate items
		Reader reader(cfg);
		ensure(reader.hasWorkingIndex());
		metadata::Collection mdc;
		reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), false), mdc);
		ensure_equals(mdc.size(), 1u);
		UItem<source::Blob> blob = mdc[0].source.upcast<source::Blob>();
		ensure_equals(blob->format, "grib1"); 
		ensure_equals(blob->filename, sys::fs::abspath("testdir/foo/bar/test.grib1"));
		ensure_equals(blob->offset, 51630u);
		ensure_equals(blob->size, 34960u);

		mdc.clear();
		reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
		ensure_equals(mdc.size(), 1u);
		blob = mdc[0].source.upcast<source::Blob>();
		ensure_equals(blob->format, "grib1"); 
		ensure_equals(blob->filename, sys::fs::abspath("testdir/foo/bar/test.grib1"));
		ensure_equals(blob->offset,  44412u);
		ensure_equals(blob->size, 7218u);
	}

	// Perform packing and check that things are still ok afterwards
	s.str(std::string());
	writer.repack(s, true);
	ensure_equals(s.str(),
		"testdir: packed foo/bar/test.grib1 (44412 saved)\n"
		"testdir: 1 file packed, 2576 bytes reclaimed on the index, 46988 total bytes freed.\n");
	c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(OK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	ensure_equals(utils::files::size("testdir/foo/bar/test.grib1"), 44412);

	// Test querying, and see that things have moved to the beginning
	Reader reader(cfg);
	ensure(reader.hasWorkingIndex());
	metadata::Collection mdc;
	reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), false), mdc);
	ensure_equals(mdc.size(), 1u);
	UItem<source::Blob> blob = mdc[0].source.upcast<source::Blob>();
	ensure_equals(blob->format, "grib1"); 
	ensure_equals(blob->filename, sys::fs::abspath("testdir/foo/bar/test.grib1"));
	ensure_equals(blob->offset, 0u);
	ensure_equals(blob->size, 34960u);

	// Query the second element and check that it starts after the first one
	// (there used to be a bug where the rebuild would use the offsets of
	// the metadata instead of the data)
	mdc.clear();
	reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
	ensure_equals(mdc.size(), 1u);
	blob = mdc[0].source.upcast<source::Blob>();
	ensure_equals(blob->format, "grib1"); 
	ensure_equals(blob->filename, sys::fs::abspath("testdir/foo/bar/test.grib1"));
	ensure_equals(blob->offset, 34960u);
	ensure_equals(blob->size, 7218u);

	// Ensure that we have the summary cache
	ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
}

// Test accuracy of maintenance scan, with index, on dataset with some
// rebuild flagfiles, and duplicate items inside
template<> template<>
void to::test<11>()
{
	acquireSamples();
	system("cat inbound/test.grib1 >> testdir/2007/07-08.grib1");
	{
		WIndex index(cfg);
		index.open();
		index.reset("2007/07-08.grib1");
	}

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 3u);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.count(TO_INDEX), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	stringstream s;

	// Perform full maintenance and check that things are still ok afterwards

	// By catting test.grib1 into 07-08.grib1, we create 2 metadata that do
	// not fit in that file (1 does).
	// Because they are duplicates of metadata in other files, one cannot
	// use the order of the data in the file to determine which one is the
	// newest. The situation cannot be fixed automatically because it is
	// impossible to determine which of the two duplicates should be thrown
	// away; therefore, we can only interrupt the maintenance and raise an
	// exception calling for manual fixing.
	try {
		writer.check(s, true, true);
		ensure(false);
	} catch (wibble::exception::Consistency& ce) {
		ensure(true);
	} catch (...) {
		ensure(false);
	}
}

// Test accuracy of maintenance scan, with index, on dataset with some
// rebuild flagfiles, and duplicate items inside
template<> template<>
void to::test<12>()
{
	acquireSamples();

	// Compress one data file
	{
		metadata::Collection mdc;
		Reader reader(cfg);
		reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
		ensure_equals(mdc.size(), 1u);
		mdc.compressDataFile(1024, "metadata file testdir/2007/07-08.grib1");
		sys::fs::deleteIfExists("testdir/2007/07-08.grib1");
	}

	// The dataset should still be clean
	{
		arki::dataset::ondisk2::Writer writer(cfg);
		MaintenanceCollector c;
		writer.maintenance(c);

		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(OK), 3u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}

	// The dataset should still be clean even with an accurate scan
	{
		arki::dataset::ondisk2::Writer writer(cfg);
		MaintenanceCollector c;
		writer.maintenance(c, false);

		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(OK), 3u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}

	// Remove the index
	system("rm testdir/index.sqlite");

	// See how maintenance scan copes
	{
		arki::dataset::ondisk2::Writer writer(cfg);
		MaintenanceCollector c;
		writer.maintenance(c);

		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(OK), 0u);
		ensure_equals(c.count(TO_INDEX), 3u);
		ensure_equals(c.remaining(), "");
		ensure(not c.isClean());

		stringstream s;

		// Perform full maintenance and check that things are still ok afterwards
		writer.check(s, true, true);
		ensure_equals(s.str(),
			"testdir: rescanned 2007/07-07.grib1\n"
			"testdir: rescanned 2007/07-08.grib1\n"
			"testdir: rescanned 2007/10-09.grib1\n"
			"testdir: 3 files rescanned, 30448 bytes reclaimed cleaning the index.\n");
		c.clear();
		writer.maintenance(c);
		ensure_equals(c.count(OK), 3u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());

		// Perform packing and check that things are still ok afterwards
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
		c.clear();

		writer.maintenance(c);
		ensure_equals(c.count(OK), 3u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());

		// Ensure that we have the summary cache
		ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
		ensure(sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
		ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
	}
}


}

// vim:set ts=4 sw=4:
