#include "segmented.h"
#include "targetfile.h"
#include "ondisk2.h"
#include "simple/writer.h"
#include "maintenance.h"
#include "archive.h"
#include "arki/metadata.h"
#include "arki/scan/any.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/files.h"
#include <sstream>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {

SegmentedReader::SegmentedReader(const ConfigFile& cfg)
    : LocalReader(cfg), m_segment_manager(segment::SegmentManager::get(cfg).release())
{
}

SegmentedReader::~SegmentedReader()
{
    if (m_segment_manager) delete m_segment_manager;
}


SegmentedWriter::SegmentedWriter(const ConfigFile& cfg)
    : LocalWriter(cfg),
      m_default_replace_strategy(REPLACE_NEVER),
      m_tf(TargetFile::create(cfg)),
      m_segment_manager(segment::SegmentManager::get(cfg).release())
{
    string repl = cfg.value("replace");
    if (repl == "yes" || repl == "true" || repl == "always")
        m_default_replace_strategy = REPLACE_ALWAYS;
    else if (repl == "USN")
        m_default_replace_strategy = REPLACE_HIGHER_USN;
    else if (repl == "" || repl == "no" || repl == "never")
        m_default_replace_strategy = REPLACE_NEVER;
    else
        throw std::runtime_error("Replace strategy '" + repl + "' is not recognised in configuration for dataset " + m_name);

    string tmp = cfg.value("archive age");
    if (!tmp.empty())
        m_archive_age = strtoul(tmp.c_str(), 0, 10);
    tmp = cfg.value("delete age");
    if (!tmp.empty())
        m_delete_age = strtoul(tmp.c_str(), 0, 10);
}

SegmentedWriter::~SegmentedWriter()
{
    if (m_segment_manager) delete m_segment_manager;
    if (m_tf) delete m_tf;
}

segment::Segment* SegmentedWriter::file(const Metadata& md, const std::string& format)
{
    string relname = (*m_tf)(md) + "." + md.source().format;
    return m_segment_manager->get_segment(format, relname);
}

void SegmentedWriter::flush()
{
    m_segment_manager->flush_writers();
}

void SegmentedWriter::archiveFile(const std::string& relpath)
{
    string pathname = str::joinpath(m_path, relpath);
    string arcrelname = str::joinpath("last", relpath);
    string arcabsname = str::joinpath(m_path, ".archive", arcrelname);
    sys::makedirs(str::dirname(arcabsname));

    // Sanity checks: avoid conflicts
    if (sys::exists(arcabsname))
    {
        stringstream ss;
        ss << "cannot archive " << pathname << " to " << arcabsname << " because the destination already exists";
        throw runtime_error(ss.str());
    }
    string src = pathname;
    string dst = arcabsname;
    bool compressed = scan::isCompressed(pathname);
    if (compressed)
    {
        src += ".gz";
        dst += ".gz";
        if (sys::exists(dst))
        {
            stringstream ss;
            ss << "cannot archive " << src << " to " << dst << " because the destination already exists";
            throw runtime_error(ss.str());
        }
    }

    // Remove stale metadata and summaries that may have been left around
    sys::unlink_ifexists(arcabsname + ".metadata");
    sys::unlink_ifexists(arcabsname + ".summary");

    // Move data to archive
    if (rename(src.c_str(), dst.c_str()) < 0)
    {
        stringstream ss;
        ss << "cannot rename " << src << " to " << dst;
        throw std::system_error(errno, std::system_category(), ss.str());
    }
    if (compressed)
        sys::rename_ifexists(pathname + ".gz.idx", arcabsname + ".gz.idx");

    // Move metadata to archive
    sys::rename_ifexists(pathname + ".metadata", arcabsname + ".metadata");
    sys::rename_ifexists(pathname + ".summary", arcabsname + ".summary");

    // Acquire in the achive
    archive().acquire(arcrelname);
}

size_t SegmentedWriter::removeFile(const std::string& relpath, bool withData)
{
    if (withData)
        return m_segment_manager->remove(relpath);
    else
        return 0;
}

void SegmentedWriter::maintenance(segment::state_func v, bool quick)
{
    if (hasArchive())
        archive().maintenance(v);
}

void SegmentedWriter::removeAll(std::ostream& log, bool writable)
{
    // TODO: decide if we're removing archives at all
    // TODO: if (hasArchive())
    // TODO:    archive().removeAll(log, writable);
}

void SegmentedWriter::sanityChecks(std::ostream& log, bool writable)
{
}

void SegmentedWriter::repack(std::ostream& log, bool writable)
{
    if (files::hasDontpackFlagfile(m_path))
    {
        log << m_path << ": dataset needs checking first" << endl;
        return;
    }

    unique_ptr<maintenance::Agent> repacker;

    if (writable)
        // No safeguard against a deleted index: we catch that in the
        // constructor and create the don't pack flagfile
        repacker.reset(new maintenance::RealRepacker(log, *this));
    else
        repacker.reset(new maintenance::MockRepacker(log, *this));
    try {
        maintenance([&](const std::string& relpath, segment::FileState state) {
            (*repacker)(relpath, state);
        });
        repacker->end();
    } catch (...) {
        files::createDontpackFlagfile(m_path);
        throw;
    }
}

void SegmentedWriter::check(std::ostream& log, bool fix, bool quick)
{
    if (fix)
    {
        maintenance::RealFixer fixer(log, *this);
        try {
            maintenance([&](const std::string& relpath, segment::FileState state) {
                fixer(relpath, state);
            }, quick);
            fixer.end();
        } catch (...) {
            files::createDontpackFlagfile(m_path);
            throw;
        }

        files::removeDontpackFlagfile(m_path);
    } else {
        maintenance::MockFixer fixer(log, *this);
        maintenance([&](const std::string& relpath, segment::FileState state) {
            fixer(relpath, state);
        }, quick);
        fixer.end();
    }

    sanityChecks(log, fix);
}

SegmentedWriter* SegmentedWriter::create(const ConfigFile& cfg)
{
    string type = str::lower(cfg.value("type"));
    if (type.empty())
        type = "local";

    if (type == "ondisk2" || type == "test")
        return new dataset::ondisk2::Writer(cfg);
    if (type == "simple" || type == "error" || type == "duplicates")
        return new dataset::simple::Writer(cfg);

    throw std::runtime_error("cannot create dataset: unknown dataset type \""+type+"\"");
}

LocalWriter::AcquireResult SegmentedWriter::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
    string type = str::lower(cfg.value("type"));
    if (type.empty())
        type = "local";

    if (type == "ondisk2" || type == "test")
        return dataset::ondisk2::Writer::testAcquire(cfg, md, out);
    if (type == "simple" || type == "error" || type == "duplicates")
        return dataset::simple::Writer::testAcquire(cfg, md, out);

    throw std::runtime_error("cannot simulate dataset acquisition: unknown dataset type \""+type+"\"");
}

}
}
