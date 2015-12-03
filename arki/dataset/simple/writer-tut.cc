#include "arki/dataset/tests.h"
#include "arki/dataset/simple/writer.h"
#include "arki/dataset/simple/reader.h"
#include "arki/types/assigneddataset.h"
#include "arki/types/source/blob.h"
#include "arki/configfile.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/matcher.h"
#include "arki/scan/grib.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"

namespace tut {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::dataset::simple;
using namespace arki::types;
using namespace arki::tests;
using namespace arki::utils;

namespace {
static inline const types::AssignedDataset* getDataset(const Metadata& md)
{
    return md.get<AssignedDataset>();
}
}

struct arki_dataset_simple_writer_shar : public DatasetTest {
	arki_dataset_simple_writer_shar()
	{
		cfg.setValue("path", "testds");
		cfg.setValue("name", "testds");
		cfg.setValue("type", "simple");
		cfg.setValue("step", "daily");
	}

	// Recreate the dataset importing data into it
	void clean_and_import(const ConfigFile* wcfg = 0, const std::string& testfile = "inbound/test.grib1")
	{
		DatasetTest::clean_and_import(wcfg, testfile);
		ensure_localds_clean(3, 3, wcfg);
	}
};
TESTGRP(arki_dataset_simple_writer);

// Test acquiring data
template<> template<>
void to::test<1>()
{
	// Clean the dataset
	system("rm -rf testds");
	system("mkdir testds");

	Metadata md;
	scan::Grib scanner;
	scanner.open("inbound/test.grib1");

	Writer writer(cfg);
	ensure(scanner.next(md));

	// Import once in the empty dataset
	WritableDataset::AcquireResult res = writer.acquire(md);
	ensure_equals(res, WritableDataset::ACQ_OK);
	#if 0
	for (vector<Note>::const_iterator i = md.notes.begin();
			i != md.notes.end(); ++i)
		cerr << *i << endl;
	#endif
    const AssignedDataset* ds = getDataset(md);
    ensure_equals(ds->name, "testds");
    ensure_equals(ds->id, "");

    wassert(actual_type(md.source()).is_source_blob("grib1", "", "testds/2007/07-08.grib1", 0, 7218));

	// Import again works fine
	res = writer.acquire(md);
	ensure_equals(res, WritableDataset::ACQ_OK);
	ds = getDataset(md);
	ensure_equals(ds->name, "testds");
	ensure_equals(ds->id, "");

    wassert(actual_type(md.source()).is_source_blob("grib1", "", "testds/2007/07-08.grib1", 7218, 7218));

    // Flush the changes and check that everything is allright
    writer.flush();
    ensure(sys::exists("testds/2007/07-08.grib1"));
    ensure(sys::exists("testds/2007/07-08.grib1.metadata"));
    ensure(sys::exists("testds/2007/07-08.grib1.summary"));
    ensure(sys::exists("testds/" + idxfname()));
    ensure(sys::timestamp("testds/2007/07-08.grib1") <= sys::timestamp("testds/2007/07-08.grib1.metadata"));
    ensure(sys::timestamp("testds/2007/07-08.grib1.metadata") <= sys::timestamp("testds/2007/07-08.grib1.summary"));
    ensure(sys::timestamp("testds/2007/07-08.grib1.summary") <= sys::timestamp("testds/" + idxfname()));
    ensure(files::hasDontpackFlagfile("testds"));

	ensure_localds_clean(1, 2);
}

// Test appending to existing files
template<> template<>
void to::test<2>()
{
	ConfigFile cfg;
	cfg.setValue("path", "testds");
	cfg.setValue("name", "testds");
	cfg.setValue("type", "simple");
	cfg.setValue("step", "yearly");

	// Clean the dataset
	system("rm -rf testds");
	system("mkdir testds");

	Metadata md;
	scan::Grib scanner;
	scanner.open("inbound/test-sorted.grib1");

	// Import once in the empty dataset
	{
		dataset::simple::Writer writer(cfg);
		ensure(scanner.next(md));

		WritableDataset::AcquireResult res = writer.acquire(md);
		ensure_equals(res, WritableDataset::ACQ_OK);
	}

	// Import another one, appending to the file
	{
		dataset::simple::Writer writer(cfg);
		ensure(scanner.next(md));

		WritableDataset::AcquireResult res = writer.acquire(md);
		ensure_equals(res, WritableDataset::ACQ_OK);

        const AssignedDataset* ds = getDataset(md);
        ensure(ds);
        ensure_equals(ds->name, "testds");
        ensure_equals(ds->id, "");

        wassert(actual_type(md.source()).is_source_blob("grib1", "", "testds/20/2007.grib1", 34960, 7218));
    }

    ensure(sys::exists("testds/20/2007.grib1"));
    ensure(sys::exists("testds/20/2007.grib1.metadata"));
    ensure(sys::exists("testds/20/2007.grib1.summary"));
    ensure(sys::exists("testds/" + idxfname()));
    ensure(sys::timestamp("testds/20/2007.grib1") <= sys::timestamp("testds/20/2007.grib1.metadata"));
    ensure(sys::timestamp("testds/20/2007.grib1.metadata") <= sys::timestamp("testds/20/2007.grib1.summary"));
    ensure(sys::timestamp("testds/20/2007.grib1.summary") <= sys::timestamp("testds/" + idxfname()));

    // Dataset is fine and clean
    ensure_localds_clean(1, 2, &cfg);
}

// Test maintenance scan on non-indexed files
template<> template<>
void to::test<3>()
{
	struct Setup {
		void operator() ()
		{
			system("rm -rf testds");
			system("mkdir testds");
			system("mkdir testds/2007");
			system("cp inbound/test-sorted.grib1 testds/2007/07-08.grib1");
		}
	} setup;

	setup();

    // Query now is ok, but empty
    {
        unique_ptr<Reader> reader(makeSimpleReader());
        metadata::Collection mdc(*reader, Matcher());
        ensure_equals(mdc.size(), 0u);
    }

	// Maintenance should show one file to index
	{
		MaintenanceCollector c;
		dataset::simple::Writer writer(cfg);
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(COUNTED_TO_INDEX), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
		ensure(files::hasDontpackFlagfile("testds"));
	}

	{
		stringstream s;
		dataset::simple::Writer writer(cfg);

		// Check should reindex the file
		writer.check(s, true, true);
		ensure_equals(s.str(),
				"testds: rescanned 2007/07-08.grib1\n"
				"testds: 1 file rescanned.\n");

		// Repack should find nothing to repack
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
		ensure(not files::hasDontpackFlagfile("testds"));
	}

	// Everything should be fine now
	ensure_localds_clean(1, 3);

	// Remove the file from the index
	{
		dataset::simple::Writer writer(cfg);
		writer.removeFile("2007/07-08.grib1", false);
	}

	// Repack should delete the files not in index
	{
		stringstream s;
		dataset::simple::Writer writer(cfg);

		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string(
				"testds: deleted 2007/07-08.grib1 (44412 freed)\n"
				"testds: 1 file deleted, 44412 total bytes freed.\n"));
	}

	// Query is still ok, but empty
	ensure_localds_clean(0, 0);
}

// Test maintenance scan with missing metadata and summary
template<> template<>
void to::test<4>()
{
    struct Setup {
        void operator() ()
        {
            sys::unlink_ifexists("testds/2007/07-08.grib1.metadata");
            sys::unlink_ifexists("testds/2007/07-08.grib1.summary");
            ensure(sys::exists("testds/2007/07-08.grib1"));
            ensure(!sys::exists("testds/2007/07-08.grib1.metadata"));
            ensure(!sys::exists("testds/2007/07-08.grib1.summary"));
        }
    } setup;

    clean_and_import();
    setup();
    ensure(sys::exists("testds/" + idxfname()));

    // Query is ok
    {
        dataset::simple::Reader reader(cfg);
        metadata::Collection mdc(reader, Matcher());
        ensure_equals(mdc.size(), 3u);
    }

	// Maintenance should show one file to rescan
	{
		MaintenanceCollector c;
		Writer writer(cfg);
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(COUNTED_OK), 2u);
		ensure_equals(c.count(COUNTED_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	// Fix the dataset
	{
		stringstream s;
		Writer writer(cfg);

		// Check should reindex the file
		writer.check(s, true, true);
		ensure_equals(s.str(),
			"testds: rescanned 2007/07-08.grib1\n"
			"testds: 1 file rescanned.\n");

		// Repack should do nothing
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}

    // Everything should be fine now
    ensure_localds_clean(3, 3);
    ensure(sys::exists("testds/2007/07-08.grib1"));
    ensure(sys::exists("testds/2007/07-08.grib1.metadata"));
    ensure(sys::exists("testds/2007/07-08.grib1.summary"));
    ensure(sys::exists("testds/" + idxfname()));


    // Restart again
    clean_and_import();
    setup();
    files::removeDontpackFlagfile("testds");
    ensure(sys::exists("testds/" + idxfname()));

	// Repack here should act as if the dataset were empty
	{
		stringstream s;
		Writer writer(cfg);

		// Repack should find nothing to repack
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}

    // And repack should have changed nothing
    {
        dataset::simple::Reader reader(cfg);
        metadata::Collection mdc(reader, Matcher());
        ensure_equals(mdc.size(), 3u);
    }
    ensure(sys::exists("testds/2007/07-08.grib1"));
    ensure(!sys::exists("testds/2007/07-08.grib1.metadata"));
    ensure(!sys::exists("testds/2007/07-08.grib1.summary"));
    ensure(sys::exists("testds/" + idxfname()));
}

// Test maintenance scan on missing summary
template<> template<>
void to::test<5>()
{
    struct Setup {
        void operator() ()
        {
            sys::unlink_ifexists("testds/2007/07-08.grib1.summary");
            ensure(sys::exists("testds/2007/07-08.grib1"));
            ensure(sys::exists("testds/2007/07-08.grib1.metadata"));
            ensure(!sys::exists("testds/2007/07-08.grib1.summary"));
        }
    } setup;

    clean_and_import();
    setup();
    ensure(sys::exists("testds/" + idxfname()));

    // Query is ok
    {
        dataset::simple::Reader reader(cfg);
        metadata::Collection mdc(reader, Matcher());
        ensure_equals(mdc.size(), 3u);
    }

	// Maintenance should show one file to rescan
	{
		MaintenanceCollector c;
		Writer writer(cfg);
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(COUNTED_OK), 2u);
		ensure_equals(c.count(COUNTED_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	// Fix the dataset
	{
		stringstream s;
		Writer writer(cfg);

		// Check should reindex the file
		writer.check(s, true, true);
		ensure_equals(s.str(),
			"testds: rescanned 2007/07-08.grib1\n"
			"testds: 1 file rescanned.\n");

		// Repack should do nothing
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}

    // Everything should be fine now
    ensure_localds_clean(3, 3);
    ensure(sys::exists("testds/2007/07-08.grib1"));
    ensure(sys::exists("testds/2007/07-08.grib1.metadata"));
    ensure(sys::exists("testds/2007/07-08.grib1.summary"));
    ensure(sys::exists("testds/" + idxfname()));


    // Restart again
    clean_and_import();
    setup();
    files::removeDontpackFlagfile("testds");
    ensure(sys::exists("testds/" + idxfname()));

	// Repack here should act as if the dataset were empty
	{
		stringstream s;
		Writer writer(cfg);

		// Repack should find nothing to repack
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}

    // And repack should have changed nothing
    {
        dataset::simple::Reader reader(cfg);
        metadata::Collection mdc(reader, Matcher());
        ensure_equals(mdc.size(), 3u);
    }
    ensure(sys::exists("testds/2007/07-08.grib1"));
    ensure(sys::exists("testds/2007/07-08.grib1.metadata"));
    ensure(!sys::exists("testds/2007/07-08.grib1.summary"));
    ensure(sys::exists("testds/" + idxfname()));
}

// Test maintenance scan on compressed archives
template<> template<>
void to::test<6>()
{
	struct Setup {
        void operator() ()
        {
            // Compress one file
            metadata::Collection mdc;
            Metadata::readFile("testds/2007/07-08.grib1.metadata", mdc);
            ensure_equals(mdc.size(), 1u);
            mdc.compressDataFile(1024, "metadata file testds/2007/07-08.grib1.metadata");
            sys::unlink_ifexists("testds/2007/07-08.grib1");

            ensure(!sys::exists("testds/2007/07-08.grib1"));
            ensure(sys::exists("testds/2007/07-08.grib1.gz"));
            ensure(sys::exists("testds/2007/07-08.grib1.gz.idx"));
            ensure(sys::exists("testds/2007/07-08.grib1.metadata"));
            ensure(sys::exists("testds/2007/07-08.grib1.summary"));
        }

        void removemd()
        {
            sys::unlink_ifexists("testds/2007/07-08.grib1.metadata");
            sys::unlink_ifexists("testds/2007/07-08.grib1.summary");
            ensure(!sys::exists("testds/2007/07-08.grib1.metadata"));
            ensure(!sys::exists("testds/2007/07-08.grib1.summary"));
        }
    } setup;

    clean_and_import();
    setup();
    ensure(sys::exists("testds/" + idxfname()));

	// Query is ok
	ensure_localds_clean(3, 3);

	// Try removing summary and metadata
	setup.removemd();

    // Cannot query anymore
    {
        metadata::Collection mdc;
        Reader reader(cfg);
        try {
            mdc.add(reader, Matcher());
            ensure(false);
        } catch (std::exception& e) {
            ensure(str::endswith(e.what(), "file needs to be manually decompressed before scanning"));
        }
    }

	// Maintenance should show one file to rescan
	{
		MaintenanceCollector c;
		Writer writer(cfg);
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(COUNTED_OK), 2u);
		ensure_equals(c.count(COUNTED_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	// Fix the dataset
	{
		stringstream s;
		Writer writer(cfg);

		// Check should reindex the file
		writer.check(s, true, true);
		ensure_equals(s.str(),
			"testds: rescanned 2007/07-08.grib1\n"
			"testds: 1 file rescanned.\n");

		// Repack should do nothing
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}

    // Everything should be fine now
    ensure_localds_clean(3, 3);
    ensure(!sys::exists("testds/2007/07-08.grib1"));
    ensure(sys::exists("testds/2007/07-08.grib1.gz"));
    ensure(sys::exists("testds/2007/07-08.grib1.gz.idx"));
    ensure(sys::exists("testds/2007/07-08.grib1.metadata"));
    ensure(sys::exists("testds/2007/07-08.grib1.summary"));
    ensure(sys::exists("testds/" + idxfname()));


    // Restart again
    clean_and_import();
    setup();
    files::removeDontpackFlagfile("testds");
    ensure(sys::exists("testds/" + idxfname()));
    setup.removemd();

	// Repack here should act as if the dataset were empty
	{
		stringstream s;
		Writer writer(cfg);

		// Repack should find nothing to repack
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}

    // And repack should have changed nothing
    {
        metadata::Collection mdc;
        Reader reader(cfg);
        try {
            mdc.add(reader, Matcher());
            ensure(false);
        } catch (std::exception& e) {
            ensure(str::endswith(e.what(), "file needs to be manually decompressed before scanning"));
        }
    }
    ensure(!sys::exists("testds/2007/07-08.grib1"));
    ensure(sys::exists("testds/2007/07-08.grib1.gz"));
    ensure(sys::exists("testds/2007/07-08.grib1.gz.idx"));
    ensure(!sys::exists("testds/2007/07-08.grib1.metadata"));
    ensure(!sys::exists("testds/2007/07-08.grib1.summary"));
    ensure(sys::exists("testds/" + idxfname()));
}

// Test maintenance scan on dataset with a file indexed but missing
template<> template<>
void to::test<7>()
{
    struct Setup {
        void operator() ()
        {
            sys::unlink_ifexists("testds/2007/07-08.grib1.summary");
            sys::unlink_ifexists("testds/2007/07-08.grib1.metadata");
            sys::unlink_ifexists("testds/2007/07-08.grib1");
        }
    } setup;

    clean_and_import();
    setup();
    ensure(sys::exists("testds/" + idxfname()));

    // Query is ok
    {
        dataset::simple::Reader reader(cfg);
        metadata::Collection mdc(reader, Matcher());
        ensure_equals(mdc.size(), 2u);
    }

	// Maintenance should show one file to rescan
	{
		MaintenanceCollector c;
		Writer writer(cfg);
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(COUNTED_OK), 2u);
		ensure_equals(c.count(COUNTED_TO_DEINDEX), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	// Fix the dataset
	{
		stringstream s;
		Writer writer(cfg);

		// Check should reindex the file
		writer.check(s, true, true);
		ensure_equals(s.str(),
			"testds: deindexed 2007/07-08.grib1\n"
			"testds: 1 file removed from index.\n");

		// Repack should do nothing
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}

    // Everything should be fine now
    ensure_localds_clean(2, 2);
    ensure(sys::exists("testds/" + idxfname()));


    // Restart again
    clean_and_import();
    setup();
    files::removeDontpackFlagfile("testds");
    ensure(sys::exists("testds/" + idxfname()));

	// Repack here should act as if the dataset were empty
	{
		stringstream s;
		Writer writer(cfg);

		// Repack should tidy up the index
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(),
			"testds: deleted from index 2007/07-08.grib1\n"
			"testds: 1 file removed from index.\n");
	}

    // And everything else should still be queriable
    ensure_localds_clean(2, 2);
    ensure(sys::exists("testds/" + idxfname()));
}

#if 0
// Test handling of empty archive dirs (such as last with everything moved away)
template<> template<>
void to::test<7>()
{
	// Import a file in a secondary archive
	{
		system("mkdir testds/.archive/foo");
		Archive arc("testds/.archive/foo");
		arc.openRW();
		system("cp inbound/test.grib1 testds/.archive/foo/");
		arc.acquire("test.grib1");
	}

	// Everything should be fine now
	Archives arc("testds/.archive");
	ensure_dataset_clean(arc, 3, 3);
}

#endif

// Retest with sqlite
template<> template<> void to::test<8>() { ForceSqlite fs; test<1>(); }
template<> template<> void to::test<9>() { ForceSqlite fs; test<2>(); }
template<> template<> void to::test<10>() { ForceSqlite fs; test<3>(); }
template<> template<> void to::test<11>() { ForceSqlite fs; test<4>(); }
template<> template<> void to::test<12>() { ForceSqlite fs; test<5>(); }
template<> template<> void to::test<13>() { ForceSqlite fs; test<6>(); }
template<> template<> void to::test<14>() { ForceSqlite fs; test<7>(); }

}
