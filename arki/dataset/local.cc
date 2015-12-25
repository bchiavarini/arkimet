#include "local.h"
#include "segmented.h"
#include "archive.h"
#include "arki/metadata.h"
#include "arki/utils/files.h"
#include "arki/configfile.h"
#include "arki/scan/any.h"
#include "arki/runtime/io.h"
#include "arki/metadata/consumer.h"
#include "arki/metadata/collection.h"
#include "arki/nag.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <iostream>
#include <fstream>
#include <sys/stat.h>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {

LocalReader::LocalReader(const ConfigFile& cfg)
    : m_name(cfg.value("name")),
      m_path(cfg.value("path")),
      m_archive(0)
{
    this->cfg = cfg.values();
}

LocalReader::~LocalReader()
{
    if (m_archive) delete m_archive;
}

bool LocalReader::hasArchive() const
{
    string arcdir = str::joinpath(m_path, ".archive");
    return sys::exists(arcdir);
}

Archives& LocalReader::archive()
{
	if (!m_archive)
		m_archive = new Archives(m_path, str::joinpath(m_path, ".archive"));
	return *m_archive;
}

void LocalReader::query_data(const dataset::DataQuery& q, std::function<bool(std::unique_ptr<Metadata>)> dest)
{
    if (hasArchive())
        archive().query_data(q, dest);
}

void LocalReader::query_summary(const Matcher& matcher, Summary& summary)
{
    if (hasArchive())
        archive().query_summary(matcher, summary);
}

size_t LocalReader::produce_nth(metadata_dest_func cons, size_t idx)
{
    if (hasArchive())
        return archive().produce_nth(cons, idx);
    return 0;
}

size_t LocalReader::scan_test(metadata_dest_func cons, size_t idx)
{
    std::map<std::string, std::string>::const_iterator i = cfg.find("filter");
    // No point in running a scan_test if there is no filter
    if (i == cfg.end())
        return 0;
    // Dataset filter that we use to validate produce_nth output
    Matcher filter = Matcher::parse(i->second);
    // Produce samples to be checked
    return produce_nth([&](unique_ptr<Metadata> md) {
        // Filter keeping only those data that, once rescanned, DO NOT match
        metadata::Collection c;

        // Inner scope to run cleanups before we produce anything
        {
            // Get the data
            const auto& data = md->getData();

            // Write the raw data to a temp file
            runtime::Tempfile tf;
            tf.write_all_or_throw((const char*)data.data(), data.size());

            // Rescan the data
            try {
                scan::scan(tf.name(), c.inserter_func(), md->source().format);
            } catch (std::exception& e) {
                // If scanning now fails, clear c so later we output the offender
                stringstream sstream;
                sstream << md->source();
                nag::verbose("%s: scanning failed: %s", sstream.str().c_str(), e.what());
                c.clear();
            }
        }

        // Check that collection has 1 element (not 0, not >1)
        if (c.size() != 1)
            return cons(move(md));

        // Match on the rescanned, if it fails, output it
        if (!filter(c[0]))
        {
            stringstream sstream;
            sstream << md->source();
            nag::verbose("%s: does not match filter");
            return cons(move(md));
        }

        // All fine, ready for the next one
        return true;
    }, idx);
}

void LocalReader::readConfig(const std::string& path, ConfigFile& cfg)
{
	if (path == "-")
	{
		// Parse the config file from stdin
		cfg.parse(cin, path);
		return;
	}

	// Remove trailing slashes, if any
	string fname = path;
	while (!fname.empty() && fname[fname.size() - 1] == '/')
		fname.resize(fname.size() - 1);

    // Check if it's a file or a directory
    std::unique_ptr<struct stat> st = sys::stat(fname);
    if (st.get() == 0)
    {
        stringstream ss;
        ss << "cannot read configuration from " << fname << " because it does not exist";
        throw runtime_error(ss.str());
    }
    if (S_ISDIR(st->st_mode))
    {
        // If it's a directory, merge in its config file
        string name = str::basename(fname);
        string file = str::joinpath(fname, "config");

        ConfigFile section;
        ifstream in;
        in.open(file.c_str(), ios::in);
        if (!in.is_open() || in.fail())
        {
            stringstream ss;
            ss << "cannot open " << file << " for reading";
            throw std::system_error(errno, std::system_category(), ss.str());
        }
        // Parse the config file into a new section
        section.parse(in, file);
        // Fill in missing bits
        section.setValue("name", name);
        section.setValue("path", sys::abspath(fname));
        // Merge into cfg
        cfg.mergeInto(name, section);
    } else {
        // If it's a file, then it's a merged config file
        ifstream in;
        in.open(fname.c_str(), ios::in);
        if (!in.is_open() || in.fail())
        {
            stringstream ss;
            ss << "cannot open config file " << fname << " for reading";
            throw std::system_error(errno, std::system_category(), ss.str());
        }
        // Parse the config file
        cfg.parse(in, fname);
    }
}

LocalWriter::LocalWriter(const ConfigFile& cfg)
    : m_path(cfg.value("path")),
      m_archive(0), m_archive_age(-1), m_delete_age(-1)
{
    m_name = cfg.value("name");
}

LocalWriter::~LocalWriter()
{
    delete m_archive;
}

bool LocalWriter::hasArchive() const
{
    string arcdir = str::joinpath(m_path, ".archive");
    return sys::exists(arcdir);
}

Archives& LocalWriter::archive()
{
	if (!m_archive)
		m_archive = new Archives(m_path, str::joinpath(m_path, ".archive"), false);
	return *m_archive;
}

const Archives& LocalWriter::archive() const
{
	if (!m_archive)
		m_archive = new Archives(m_path, str::joinpath(m_path, ".archive"), false);
	return *m_archive;
}

LocalWriter* LocalWriter::create(const ConfigFile& cfg)
{
    return SegmentedWriter::create(cfg);
}

LocalWriter::AcquireResult LocalWriter::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
    return SegmentedWriter::testAcquire(cfg, md, out);
}

}
}
