/*
 * dataset/index/manifest - Index files with no duplicate checks
 *
 * Copyright (C) 2009--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "arki/libconfig.h"
#include "arki/dataset/index/manifest.h"
#include "arki/dataset/maintenance.h"
#include "arki/metadata/collection.h"
#include "arki/metadata/consumer.h"
#include "arki/types/source/blob.h"
#include "arki/configfile.h"
#include "arki/summary.h"
#include "arki/types/reftime.h"
#include "arki/matcher.h"
#include "arki/scan/dir.h"
#include "arki/utils/sqlite.h"
#include "arki/utils/files.h"
#include "arki/utils/dataset.h"
#include "arki/utils/compress.h"
#include "arki/sort.h"
#include "arki/scan/any.h"
#include "arki/nag.h"
#include "arki/iotrace.h"

#include <wibble/exception.h>
#include <wibble/sys/fs.h>
#include <wibble/string.h>

#include <unistd.h>
#include <fstream>
#include <ctime>

#ifdef HAVE_LUA
#include <arki/report.h>
#endif

using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::utils::sqlite;
using namespace arki::dataset::maintenance;

namespace arki {
namespace dataset {
namespace index {

namespace {
void scan_file(data::SegmentManager& sm, const std::string& root, const std::string& relname, MaintFileVisitor& visitor, bool quick=true)
{
    struct HFSorter : public sort::Compare
    {
        virtual int compare(const Metadata& a, const Metadata& b) const {
            int res = Type::nullable_compare(a.get(TYPE_REFTIME), b.get(TYPE_REFTIME));
            if (res == 0)
                return a.source().compare(b.source());
            return res;
        }
        virtual std::string toString() const {
            return "HFSorter";
        }
    } cmp;

    string absname = str::joinpath(root, relname);

    // If the data file is compressed, create a temporary uncompressed copy
    auto_ptr<utils::compress::TempUnzip> tu;
    if (!quick && scan::isCompressed(absname))
        tu.reset(new utils::compress::TempUnzip(absname));

    metadata::Collection mdc;
    scan::scan(absname, mdc);
    //mdc.sort(""); // Sort by reftime, to find items out of order
    mdc.sort(cmp); // Sort by reftime and by offset

    // Check the state of the file
    data::FileState state = sm.check(relname, mdc, quick);

    // Pass on the file state to the visitor
    visitor(relname, state);
}
}


Manifest::Manifest(const ConfigFile& cfg) : m_path(cfg.value("path")) {}
Manifest::Manifest(const std::string& path) : m_path(path) {}
Manifest::~Manifest() {}

void Manifest::querySummaries(const Matcher& matcher, Summary& summary)
{
	vector<string> files;
	fileList(matcher, files);

	for (vector<string>::const_iterator i = files.begin(); i != files.end(); ++i)
	{
		string pathname = str::joinpath(m_path, *i);

		// Silently skip files that have been deleted
		if (!sys::fs::access(pathname + ".summary", R_OK))
			continue;

		Summary s;
		s.readFile(pathname + ".summary");
		s.filter(matcher, summary);
	}
}

namespace {
// Tweak Blob sources replacing basedir and prepending a directory to the file name
struct FixSource : public metadata::Eater
{
    string basedir;
    string prepend_fname;
    metadata::Eater& next;

    FixSource(metadata::Eater& next) : next(next) {}

    bool eat(auto_ptr<Metadata> md) override
    {
        if (const source::Blob* s = md->has_source_blob())
            md->set_source(Source::createBlob(s->format, basedir, str::joinpath(prepend_fname, s->filename), s->offset, s->size));
        return next.eat(md);
    }
};
}

void Manifest::queryData(const dataset::DataQuery& q, metadata::Eater& consumer)
{
	vector<string> files;
	fileList(q.matcher, files);

	// TODO: does it make sense to check with the summary first?

    metadata::Eater* c = &consumer;
    // Order matters here, as delete will happen in reverse order
    refcounted::Pointer<sort::Compare> compare;

    if (q.sorter)
        compare = q.sorter;
    else
        // If no sorter is provided, sort by reftime in case data files have
        // not been sorted before archiving
        compare = sort::Compare::parse("reftime");

    string absdir = sys::fs::abspath(m_path);
    sort::Stream sorter(*compare, *c);
    //ds::MakeAbsolute mkabs(sorter);
    FixSource fs(sorter);
    fs.basedir = absdir;
    metadata::FilteredEater filter(q.matcher, fs);
    for (vector<string>::const_iterator i = files.begin(); i != files.end(); ++i)
    {
        fs.prepend_fname = str::dirname(*i);
        string fullpath = str::joinpath(absdir, *i);
        if (!scan::exists(fullpath)) continue;
        // This generates filenames relative to the metadata
        // We need to use absdir as the dirname, and prepend dirname(*i) to the filenames
        scan::scan(absdir, *i, filter);
        sorter.flush();
    }
}

void Manifest::querySummary(const Matcher& matcher, Summary& summary)
{
	// Check if the matcher discriminates on reference times
	const matcher::Implementation* rtmatch = 0;
	if (matcher.m_impl)
		rtmatch = matcher.m_impl->get(types::TYPE_REFTIME);

	if (!rtmatch)
	{
		// The matcher does not contain reftime, we can work with a
		// global summary
		string cache_pathname = str::joinpath(m_path, "summary");

		if (sys::fs::access(cache_pathname, R_OK))
		{
			Summary s;
			s.readFile(cache_pathname);
			s.filter(matcher, summary);
		} else if (sys::fs::access(m_path, W_OK)) {
			// Rebuild the cache
			Summary s;
			querySummaries(Matcher(), s);

			// Save the summary
			s.writeAtomically(cache_pathname);

			// Query the newly generated summary that we still have
			// in memory
			s.filter(matcher, summary);
		} else
			querySummaries(matcher, summary);
	} else {
		querySummaries(matcher, summary);
	}
}

namespace {
struct NthFilter : public metadata::Eater
{
    metadata::Eater& next;
    size_t idx;

    NthFilter(metadata::Eater& next, size_t idx)
        : next(next), idx(idx+1) {}

    bool eat(auto_ptr<Metadata> md) override
    {
        switch (idx)
        {
            case 0: return false;
            case 1: next.eat(md); --idx; return false;
            default: --idx; return true;
        }
    }
    bool produced() const { return idx == 0; }
};
}

size_t Manifest::produce_nth(metadata::Eater& cons, size_t idx)
{
    size_t res = 0;
    // List all files
    vector<string> files;
    fileList(Matcher(), files);

    string absdir = sys::fs::abspath(m_path);
    //ds::MakeAbsolute mkabs(cons);
    FixSource fs(cons);
    fs.basedir = absdir;
    for (vector<string>::const_iterator i = files.begin(); i != files.end(); ++i)
    {
        fs.prepend_fname = str::dirname(*i);
        string fullpath = str::joinpath(absdir, *i);
        if (!scan::exists(fullpath)) continue;
        NthFilter filter(cons, idx);
        scan::scan(absdir, *i, filter);
        if (filter.produced())
            ++res;
    }

    return res;
}

void Manifest::invalidate_summary()
{
    sys::fs::deleteIfExists(str::joinpath(m_path, "summary"));
}

void Manifest::invalidate_summary(const std::string& relname)
{
    sys::fs::deleteIfExists(str::joinpath(m_path, relname) + ".summary");
    invalidate_summary();
}

void Manifest::rescanFile(const std::string& dir, const std::string& relpath)
{
	string pathname = str::joinpath(dir, relpath);

	// Temporarily uncompress the file for scanning
	auto_ptr<utils::compress::TempUnzip> tu;
	if (scan::isCompressed(pathname))
		tu.reset(new utils::compress::TempUnzip(pathname));

	// Read the timestamp
	time_t mtime = sys::fs::timestamp(pathname);

    // Invalidate summary
    invalidate_summary(pathname);

	// Invalidate metadata if older than data
	time_t ts_md = sys::fs::timestamp(pathname + ".metadata", 0);
	if (ts_md < mtime)
		sys::fs::deleteIfExists(pathname + ".metadata");

	// Scan the file
	metadata::Collection mds;
	if (!scan::scan(pathname, mds))
		throw wibble::exception::Consistency("rescanning " + pathname, "it does not look like a file we can scan");

	// Iterate the metadata, computing the summary and making the data
	// paths relative
	Summary sum;
    for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
    {
        const source::Blob& s = (*i)->sourceBlob();
        (*i)->set_source(upcast<Source>(s.fileOnly()));
        sum.add(**i);
    }

	// Regenerate .metadata
	mds.writeAtomically(pathname + ".metadata");

	// Regenerate .summary
	sum.writeAtomically(pathname + ".summary");

	// Add to manifest
	acquire(relpath, mtime, sum);
}

namespace manifest {
static bool mft_force_sqlite = false;

static bool sorter(const std::string& a, const std::string& b)
{
	return b < a;
}

class PlainManifest : public Manifest
{
	struct Info
	{
		std::string file;
		time_t mtime;
        types::Time start_time;
        types::Time end_time;

		bool operator<(const Info& i) const
		{
			return file < i.file;
		}

		bool operator==(const Info& i) const
		{
			return file == i.file;
		}

		bool operator!=(const Info& i) const
		{
			return file != i.file;
		}

		void write(ostream& out) const
		{
			out << file << ";" << mtime << ";" << start_time.toSQL() << ";" << end_time.toSQL() << endl;
		}
	};
	vector<Info> info;
	ino_t last_inode;
	bool dirty;
    bool rw;

	/**
	 * Reread the MANIFEST file.
	 *
	 * @returns true if the MANIFEST file existed, false if not
	 */
	bool reread()
	{
		string pathname(str::joinpath(m_path, "MANIFEST"));
		ino_t inode = sys::fs::inode(pathname, 0);

		if (inode == last_inode) return inode != 0;

		info.clear();
		last_inode = inode;
		if (last_inode == 0)
			return false;

		std::ifstream in;
		in.open(pathname.c_str(), ios::in);
		if (!in.is_open() || in.fail())
			throw wibble::exception::File(pathname, "opening file for reading");

        iotrace::trace_file(pathname, 0, 0, "read MANIFEST");

		string line;
		for (size_t lineno = 1; !in.eof(); ++lineno)
		{
			Info item;

			getline(in, line);
			if (in.fail() && !in.eof())
				throw wibble::exception::File(pathname, "reading one line");

			// Skip empty lines
			if (line.empty()) continue;

			size_t beg = 0;
			size_t end = line.find(';');
			if (end == string::npos)
				throw wibble::exception::Consistency("parsing " + pathname + ":" + str::fmt(lineno),
						"line has only 1 field");

			item.file = line.substr(beg, end-beg);
			
			beg = end + 1;
			end = line.find(';', beg);
			if (end == string::npos)
				throw wibble::exception::Consistency("parsing " + pathname + ":" + str::fmt(lineno),
						"line has only 2 fields");

			item.mtime = strtoul(line.substr(beg, end-beg).c_str(), 0, 10);

			beg = end + 1;
			end = line.find(';', beg);
			if (end == string::npos)
				throw wibble::exception::Consistency("parsing " + pathname + ":" + str::fmt(lineno),
						"line has only 3 fields");

            item.start_time.setFromSQL(line.substr(beg, end-beg));
            item.end_time.setFromSQL(line.substr(end+1));

            info.push_back(item);
        }

		in.close();
		dirty = false;
		return true;
	}

public:
	PlainManifest(const std::string& dir)
		: Manifest(dir), last_inode(0), dirty(false), rw(false)
	{
	}

	virtual ~PlainManifest()
	{
		flush();
	}

    void openRO()
    {
        if (!reread())
            throw wibble::exception::Consistency("opening archive index", "MANIFEST does not exist in " + m_path);
        rw = false;
    }

    void openRW()
    {
        if (!reread())
            dirty = true;
        rw = true;
    }

	void fileList(const Matcher& matcher, std::vector<std::string>& files)
	{
		reread();

        string query;
        Time begin;
        Time end;
		if (matcher.date_extremes(begin, end))
		{
            // Get files with matching reftime
            for (vector<Info>::const_iterator i = info.begin();
                    i != info.end(); ++i)
            {
                if (begin.isValid() && i->end_time < begin) continue;
                if (end.isValid() && i->start_time > end) continue;
                files.push_back(i->file);
            }
		} else {
			// No restrictions on reftime: get all files
			for (vector<Info>::const_iterator i = info.begin();
					i != info.end(); ++i)
				files.push_back(i->file);
		}
	}

    void fileTimespan(const std::string& relname, Time& start_time, Time& end_time) const override
    {
		// Lookup the file (FIXME: reimplement binary search so we
		// don't need to create a temporary Info)
		Info sample;
		sample.file = relname;
		vector<Info>::const_iterator lb = lower_bound(info.begin(), info.end(), sample);
		if (lb != info.end() && lb->file == relname)
		{
			start_time = lb->start_time;
			end_time = lb->end_time;
        } else {
            start_time.setInvalid();
            end_time.setInvalid();
        }
    }

    bool date_extremes(Time& begin, Time& end) const override
    {
        for (vector<Info>::const_iterator i = info.begin(); i != info.end(); ++i)
        {
            if (!begin.isValid() || (i->start_time.isValid() && i->start_time < begin))
                begin = i->start_time;
            if (!end.isValid() || (i->end_time.isValid() && i->end_time > end))
                end = i->end_time;
        }
        return !info.empty();
    }

	void vacuum()
	{
	}

    Pending test_writelock()
    {
        return Pending();
    }

	void acquire(const std::string& relname, time_t mtime, const Summary& sum)
	{
		reread();

		Info item;
		item.file = relname;
		item.mtime = mtime;

        // Add to index
        auto_ptr<Reftime> rt = sum.getReferenceTime();

        string bt;
        string et;

        switch (rt->style())
        {
            case types::Reftime::POSITION: {
                const reftime::Position* p = dynamic_cast<const reftime::Position*>(rt.get());
                item.start_time = item.end_time = p->time;
                break;
            }
            case types::Reftime::PERIOD: {
                const reftime::Period* p = dynamic_cast<const reftime::Period*>(rt.get());
                item.start_time = p->begin;
                item.end_time = p->end;
                break;
            }
            default:
                throw wibble::exception::Consistency("unsupported reference time " + types::Reftime::formatStyle(rt->style()));
        }

		// Insertion sort; at the end, everything is already sorted and we
		// avoid inserting lots of duplicate items
		vector<Info>::iterator lb = lower_bound(info.begin(), info.end(), item);
		if (lb == info.end())
			info.push_back(item);
		else if (*lb != item)
			info.insert(lb, item);
		else
			*lb = item;

		dirty = true;
	}

	virtual void remove(const std::string& relname)
	{
		reread();

		vector<Info>::iterator i;
		for (i = info.begin(); i != info.end(); ++i)
			if (i->file == relname)
				break;
		if (i != info.end())
			info.erase(i);

		dirty = true;
	}

	virtual void check(data::SegmentManager& sm, MaintFileVisitor& v, bool quick=true)
	{
#if 0
	// TODO: run file:///usr/share/doc/sqlite3-doc/pragma.html#debug
	// and delete the index if it fails

	// Iterate subdirs in sorted order
	// Also iterate files on index in sorted order
	// Check each file for need to reindex or repack
	writer::CheckAge ca(v, m_idx, m_archive_age, m_delete_age);
	vector<string> files = scan::dir(m_path);
	maintenance::FindMissing fm(ca, files);
	maintenance::HoleFinder hf(fm, m_path, quick);
	m_idx.scan_files(hf);
	hf.end();
	fm.end();
	if (hasArchive())
		archive().maintenance(v);
#endif
		reread();

		// List of files existing on disk
		std::vector<std::string> disk = scan::dir(m_path, true);
		std::sort(disk.begin(), disk.end(), sorter);

		vector<Info> info = this->info;
		for (vector<Info>::const_iterator i = info.begin(); i != info.end(); ++i)
		{
			while (not disk.empty() and disk.back() < i->file)
			{
				nag::verbose("%s: %s is not in index", m_path.c_str(), disk.back().c_str());
				v(disk.back(), FILE_TO_INDEX);
				disk.pop_back();
			}
			if (not disk.empty() and disk.back() == i->file)
			{
				disk.pop_back();

				string pathname = str::joinpath(m_path, i->file);

				time_t ts_data = scan::timestamp(pathname);
				time_t ts_md = sys::fs::timestamp(pathname + ".metadata", 0);
				time_t ts_sum = sys::fs::timestamp(pathname + ".summary", 0);
				time_t ts_idx = i->mtime;

				if (ts_idx != ts_data ||
				    ts_md < ts_data ||
				    ts_sum < ts_md)
				{
					// Check timestamp consistency
					if (ts_idx != ts_data)
						nag::verbose("%s: %s has a timestamp (%d) different than the one in the index (%d)",
								m_path.c_str(), i->file.c_str(), ts_data, ts_idx);
					if (ts_md < ts_data)
						nag::verbose("%s: %s has a timestamp (%d) newer that its metadata (%d)",
								m_path.c_str(), i->file.c_str(), ts_data, ts_md);
					if (ts_md < ts_data)
						nag::verbose("%s: %s metadata has a timestamp (%d) newer that its summary (%d)",
								m_path.c_str(), i->file.c_str(), ts_md, ts_sum);
					v(i->file, FILE_TO_RESCAN);
				}
				else
                    scan_file(sm, m_path, i->file, v, quick);
			}
			else // if (disk.empty() or disk.back() > i->file)
			{
				nag::verbose("%s: %s has been deleted from the archive", m_path.c_str(), i->file.c_str());
				v(i->file, FILE_TO_DEINDEX);
			}
		}
		while (not disk.empty())
		{
			nag::verbose("%s: %s is not in index", m_path.c_str(), disk.back().c_str());
			v(disk.back(), FILE_TO_INDEX);
			disk.pop_back();
		}
	}

	void flush()
	{
        if (dirty)
        {
            string pathname(str::joinpath(m_path, "MANIFEST.tmp"));

            std::ofstream out;
            out.open(pathname.c_str(), ios::out);
            if (!out.is_open() || out.fail())
                throw wibble::exception::File(pathname, "opening file for writing");

            for (vector<Info>::const_iterator i = info.begin();
                    i != info.end(); ++i)
                i->write(out);

            out.close();

            if (::rename(pathname.c_str(), str::joinpath(m_path, "MANIFEST").c_str()) < 0)
                throw wibble::exception::System("Renaming " + pathname + " to " + str::joinpath(m_path, "MANIFEST"));

            invalidate_summary();
            dirty = false;
        }

        if (rw && ! sys::fs::exists(str::joinpath(m_path, "summary")))
        {
            Summary s;
            querySummary(Matcher(), s);
        }
    }

	static bool exists(const std::string& dir)
	{
		string pathname(str::joinpath(dir, "MANIFEST"));
		return wibble::sys::fs::access(pathname, F_OK);
	}
};


class SqliteManifest : public Manifest
{
	mutable utils::sqlite::SQLiteDB m_db;
	utils::sqlite::InsertQuery m_insert;

	void setupPragmas()
	{
		// Also, since the way we do inserts cause no trouble if a reader reads a
		// partial insert, we do not need read locking
		//m_db.exec("PRAGMA read_uncommitted = 1");
		// Use new features, if we write we read it, so we do not need to
		// support sqlite < 3.3.0 if we are above that version
		m_db.exec("PRAGMA legacy_file_format = 0");
        // Enable WAL journaling, which doesn't lock reads while writing
		m_db.exec("PRAGMA journal_mode = WAL");
	}

	void initQueries()
	{
		m_insert.compile("INSERT OR REPLACE INTO files (file, mtime, start_time, end_time) VALUES (?, ?, ?, ?)");
	}

	void initDB()
	{
		// Create the main table
		string query = "CREATE TABLE IF NOT EXISTS files ("
			"id INTEGER PRIMARY KEY,"
			" file TEXT NOT NULL,"
			" mtime INTEGER NOT NULL,"
			" start_time TEXT NOT NULL,"
			" end_time TEXT NOT NULL,"
			" UNIQUE(file) )";
		m_db.exec(query);
		m_db.exec("CREATE INDEX idx_files_start ON files (start_time)");
		m_db.exec("CREATE INDEX idx_files_end ON files (end_time)");
	}


public:
	SqliteManifest(const std::string& dir)
		: Manifest(dir), m_insert(m_db)
	{
	}

	virtual ~SqliteManifest()
	{
	}

	void flush()
	{
        // Not needed for index data consistency, but we need it to ensure file
        // timestamps are consistent at this point.
        m_db.checkpoint();
	}

	void openRO()
	{
		string pathname(str::joinpath(m_path, "index.sqlite"));
		if (m_db.isOpen())
			throw wibble::exception::Consistency("opening archive index", "index " + pathname + " is already open");

		if (!wibble::sys::fs::access(pathname, F_OK))
			throw wibble::exception::Consistency("opening archive index", "index " + pathname + " does not exist");

		m_db.open(pathname);
		setupPragmas();

		initQueries();
	}

	void openRW()
	{
		string pathname(str::joinpath(m_path, "index.sqlite"));
		if (m_db.isOpen())
			throw wibble::exception::Consistency("opening archive index", "index " + pathname + " is already open");

		bool need_create = !wibble::sys::fs::access(pathname, F_OK);

		m_db.open(pathname);
		setupPragmas();
		
		if (need_create)
			initDB();

		initQueries();
	}

    void fileList(const Matcher& matcher, std::vector<std::string>& files) override
    {
        string query;
        Time begin;
        Time end;
		if (matcher.date_extremes(begin, end))
		{
			query = "SELECT file FROM files";

			if (begin.isValid())
				query += " WHERE end_time >= '" + begin.toSQL() + "'";
			if (end.isValid())
			{
				if (begin.isValid())
					query += " AND start_time <= '" + end.toSQL() + "'";
				else
					query += " WHERE start_time <= '" + end.toSQL() + "'";
			}

			query += " ORDER BY file";
		} else {
			// No restrictions on reftime: get all files
			query = "SELECT file FROM files ORDER BY file";
		}

		// cerr << "Query: " << query << endl;
		
		Query q("sel_archive", m_db);
		q.compile(query);
		while (q.step())
			files.push_back(q.fetchString(0));
	}

    void fileTimespan(const std::string& relname, Time& start_time, Time& end_time) const override
    {
		Query q("sel_file_ts", m_db);
		q.compile("SELECT start_time, end_time FROM files WHERE file=?");
		q.bind(1, relname);

        start_time.setInvalid();
        end_time.setInvalid();
        while (q.step())
        {
            start_time.setFromSQL(q.fetchString(0));
            end_time.setFromSQL(q.fetchString(1));
        }
    }

    bool date_extremes(Time& begin, Time& end) const override
    {
		Query q("sel_date_extremes", m_db);
		q.compile("SELECT MIN(start_time), MAX(end_time) FROM files");

        bool found = false;
        Time st;
        Time et;
        while (q.step())
        {
            st.setFromSQL(q.fetchString(0));
            et.setFromSQL(q.fetchString(1));

            if (!begin.isValid() || (st.isValid() && st < begin))
                begin = st;
            if (!end.isValid() || (et.isValid() && et > end))
                end = et;

            found = true;
        }
        return found;
    }

	void vacuum()
	{
		// Vacuum the database
		try {
			m_db.exec("VACUUM");
			m_db.exec("ANALYZE");
		} catch (std::exception& e) {
			nag::warning("ignoring failed attempt to optimize database: %s", e.what());
		}
	}

    Pending test_writelock()
    {
        return Pending(new SqliteTransaction(m_db, true));
    }

	void acquire(const std::string& relname, time_t mtime, const Summary& sum)
	{
        // Add to index
        auto_ptr<types::Reftime> rt = sum.getReferenceTime();

		string bt;
		string et;

        switch (rt->style())
        {
            case types::Reftime::POSITION: {
                const reftime::Position* p = dynamic_cast<const reftime::Position*>(rt.get());
                bt = et = p->time.toSQL();
                break;
            }
            case types::Reftime::PERIOD: {
                const reftime::Period* p = dynamic_cast<const reftime::Period*>(rt.get());
                bt = p->begin.toSQL();
                et = p->end.toSQL();
                break;
            }
            default:
                    throw wibble::exception::Consistency("unsupported reference time " + types::Reftime::formatStyle(rt->style()));
        }

		m_insert.reset();
		m_insert.bind(1, relname);
		m_insert.bind(2, mtime);
		m_insert.bind(3, bt);
		m_insert.bind(4, et);
		m_insert.step();
	}

	virtual void remove(const std::string& relname)
	{
		Query q("del_file", m_db);
		q.compile("DELETE FROM files WHERE file=?");
		q.bind(1, relname);
		while (q.step())
			;
	}

	virtual void check(data::SegmentManager& sm, MaintFileVisitor& v, bool quick=true)
	{
		// List of files existing on disk
		std::vector<std::string> disk = scan::dir(m_path, true);
		std::sort(disk.begin(), disk.end(), sorter);

		// Preread the file list, so it does not get modified as we scan
		vector< pair<string, time_t> > files;
		{
			Query q("sel_archive", m_db);
			q.compile("SELECT file, mtime FROM files ORDER BY file");

			while (q.step())
				files.push_back(make_pair(q.fetchString(0), q.fetch<time_t>(1)));
		}

		for (vector< pair<string, time_t> >::const_iterator i = files.begin(); i != files.end(); ++i)
		{
			while (not disk.empty() and disk.back() < i->first)
			{
				nag::verbose("%s: %s is not in index", m_path.c_str(), disk.back().c_str());
				v(disk.back(), FILE_TO_INDEX);
				disk.pop_back();
			}
			if (not disk.empty() and disk.back() == i->first)
			{
				disk.pop_back();

				string pathname = str::joinpath(m_path, i->first);

				time_t ts_data = scan::timestamp(pathname);
				time_t ts_md = sys::fs::timestamp(pathname + ".metadata", 0);
				time_t ts_sum = sys::fs::timestamp(pathname + ".summary", 0);
				time_t ts_idx = i->second;

				if (ts_idx != ts_data ||
				    ts_md < ts_data ||
				    ts_sum < ts_md)
				{
					// Check timestamp consistency
					if (ts_idx != ts_data)
						nag::verbose("%s: %s has a timestamp (%d) different than the one in the index (%d)",
								m_path.c_str(), i->first.c_str(), ts_data, ts_idx);
					if (ts_md < ts_data)
						nag::verbose("%s: %s has a timestamp (%d) newer that its metadata (%d)",
								m_path.c_str(), i->first.c_str(), ts_data, ts_md);
					if (ts_md < ts_data)
						nag::verbose("%s: %s metadata has a timestamp (%d) newer that its summary (%d)",
								m_path.c_str(), i->first.c_str(), ts_md, ts_sum);
					v(i->first, FILE_TO_RESCAN);
				}
				else
                    scan_file(sm, m_path, i->first, v, quick);
			}
			else // if (disk.empty() or disk.back() > i->first)
			{
				nag::verbose("%s: %s has been deleted from the archive", m_path.c_str(), i->first.c_str());
				v(i->first, FILE_TO_DEINDEX);
			}
		}
		while (not disk.empty())
		{
			nag::verbose("%s: %s is not in index", m_path.c_str(), disk.back().c_str());
			v(disk.back(), FILE_TO_INDEX);
			disk.pop_back();
		}
	}

	static bool exists(const std::string& dir)
	{
		string pathname(str::joinpath(dir, "index.sqlite"));
		return wibble::sys::fs::access(pathname, F_OK);
	}
};

}

bool Manifest::get_force_sqlite()
{
	return manifest::mft_force_sqlite;
}

void Manifest::set_force_sqlite(bool val)
{
	manifest::mft_force_sqlite = val;
}

bool Manifest::exists(const std::string& dir)
{
	return manifest::PlainManifest::exists(dir) ||
	       manifest::SqliteManifest::exists(dir);
}

std::auto_ptr<Manifest> Manifest::create(const std::string& dir, const ConfigFile* cfg)
{
    std::string value;
    if (cfg) value = cfg->value("index_type");
    if (value.empty())
    {
        if (manifest::mft_force_sqlite || manifest::SqliteManifest::exists(dir))
            return auto_ptr<Manifest>(new manifest::SqliteManifest(dir));
        else
            return auto_ptr<Manifest>(new manifest::PlainManifest(dir));
    }
    else if (value == "plain")
        return auto_ptr<Manifest>(new manifest::PlainManifest(dir));
    else if (value == "sqlite")
        return auto_ptr<Manifest>(new manifest::SqliteManifest(dir));
    else
        throw wibble::exception::Consistency("unsupported index_type " + value);
}

}
}
}
