#include "arki/dataset/iseg/checker.h"
#include "arki/dataset/iseg/index.h"
#include "arki/dataset/maintenance.h"
#include "arki/dataset/segment.h"
#include "arki/dataset/step.h"
#include "arki/dataset/reporter.h"
#include "arki/dataset/time.h"
#include "arki/dataset/lock.h"
#include "arki/types/source/blob.h"
#include "arki/summary.h"
#include "arki/types/reftime.h"
#include "arki/matcher.h"
#include "arki/metadata/collection.h"
#include "arki/utils/files.h"
#include "arki/sort.h"
#include "arki/nag.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/binary.h"
#include <system_error>
#include <algorithm>
#include <cstring>

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace iseg {

namespace {

/// Create unique strings from metadata
struct IDMaker
{
    std::set<types::Code> components;

    IDMaker(const std::set<types::Code>& components) : components(components) {}

    vector<uint8_t> make_string(const Metadata& md) const
    {
        vector<uint8_t> res;
        BinaryEncoder enc(res);
        for (set<types::Code>::const_iterator i = components.begin(); i != components.end(); ++i)
            if (const Type* t = md.get(*i))
                t->encodeBinary(enc);
        return res;
    }
};

}


class CheckerSegment : public segmented::CheckerSegment
{
public:
    Checker& checker;
    CIndex* m_idx = nullptr;

    CheckerSegment(Checker& checker, const std::string& relpath)
        : CheckerSegment(checker, relpath, checker.config().check_lock_segment(relpath)) {}
    CheckerSegment(Checker& checker, const std::string& relpath, std::shared_ptr<dataset::CheckLock> lock)
        : segmented::CheckerSegment(lock), checker(checker)
    {
        segment = checker.segment_manager().get_checker(checker.config().format, relpath);
    }
    ~CheckerSegment()
    {
        delete m_idx;
    }

    CIndex& idx()
    {
        if (!m_idx)
            m_idx = new CIndex(checker.m_config, path_relative(), lock);
        return *m_idx;
    }
    std::string path_relative() const override { return segment->segment().relpath; }
    const iseg::Config& config() const override { return checker.config(); }
    dataset::ArchivesChecker& archives() const { return checker.archive(); }

    void get_metadata(std::shared_ptr<core::Lock> lock, metadata::Collection& mds) override
    {
        idx().scan(checker.segment_manager(), mds.inserter_func(), "reftime, offset");
    }

    segmented::SegmentState scan(dataset::Reporter& reporter, bool quick=true) override
    {
        if (!segment->exists_on_disk())
            return segmented::SegmentState(segment::SEGMENT_MISSING);

        if (!sys::stat(str::joinpath(checker.config().path, segment->segment().relpath + ".index")))
        {
            if (segment->is_empty())
            {
                reporter.segment_info(checker.name(), segment->segment().relpath, "empty segment found on disk with no associated index data");
                return segmented::SegmentState(segment::SEGMENT_DELETED);
            } else {
                //bool untrusted_index = files::hasDontpackFlagfile(checker.config().path);
                reporter.segment_info(checker.name(), segment->segment().relpath, "segment found on disk with no associated index data");
                //return segmented::SegmentState(untrusted_index ? segment::SEGMENT_UNALIGNED : segment::SEGMENT_DELETED);
                return segmented::SegmentState(segment::SEGMENT_UNALIGNED);
            }
        }

#if 0
        /**
         * Although iseg could detect if the data of a segment is newer than its
         * index, the timestamp of the index is updated by various kinds of sqlite
         * operations, making the test rather useless, because it's likely that the
         * index timestamp would get updated before the mismatch is detected.
         */
        string abspath = str::joinpath(config().path, relpath);
        if (sys::timestamp(abspath) > sys::timestamp(abspath + ".index"))
        {
            segments_state.insert(make_pair(relpath, segmented::SegmentState(segment::SEGMENT_UNALIGNED)));
            return;
        }
#endif

        metadata::Collection mds;
        idx().scan(checker.segment_manager(), mds.inserter_func(), "reftime, offset");
        segment::State state = segment::SEGMENT_OK;

        // Compute the span of reftimes inside the segment
        unique_ptr<core::Time> md_begin;
        unique_ptr<core::Time> md_until;
        if (mds.empty())
        {
            reporter.segment_info(checker.name(), segment->segment().relpath, "index knows of this segment but contains no data for it");
            md_begin.reset(new core::Time(0, 0, 0));
            md_until.reset(new core::Time(0, 0, 0));
            state = segment::SEGMENT_DELETED;
        } else {
            if (!mds.expand_date_range(md_begin, md_until))
            {
                reporter.segment_info(checker.name(), segment->segment().relpath, "index data for this segment has no reference time information");
                state = segment::SEGMENT_CORRUPTED;
                md_begin.reset(new core::Time(0, 0, 0));
                md_until.reset(new core::Time(0, 0, 0));
            } else {
                // Ensure that the reftime span fits inside the segment step
                core::Time seg_begin;
                core::Time seg_until;
                if (checker.config().step().path_timespan(segment->segment().relpath, seg_begin, seg_until))
                {
                    if (*md_begin < seg_begin || *md_until > seg_until)
                    {
                        reporter.segment_info(checker.name(), segment->segment().relpath, "segment contents do not fit inside the step of this dataset");
                        state = segment::SEGMENT_CORRUPTED;
                    }
                    // Expand segment timespan to the full possible segment timespan
                    *md_begin = seg_begin;
                    *md_until = seg_until;
                } else {
                    reporter.segment_info(checker.name(), segment->segment().relpath, "segment name does not fit the step of this dataset");
                    state = segment::SEGMENT_CORRUPTED;
                }
            }
        }

        if (state.is_ok())
            state = segment->check([&](const std::string& msg) { reporter.segment_info(checker.name(), segment->segment().relpath, msg); }, mds, quick);

        auto res = segmented::SegmentState(state, *md_begin, *md_until);
        res.check_age(segment->segment().relpath, checker.config(), reporter);
        return res;
    }

    void tar() override
    {
        if (sys::exists(segment->segment().abspath + ".tar"))
            return;

        auto write_lock = lock->write_lock();
        Pending p = idx().begin_transaction();

        // Rescan file
        metadata::Collection mds;
        idx().scan(checker.segment_manager(), mds.inserter_func(), "reftime, offset");

        // Create the .tar segment
        segment = segment->tar(mds);

        // Reindex the new metadata
        idx().reset();
        for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
        {
            const source::Blob& source = (*i)->sourceBlob();
            if (idx().index(**i, source.offset))
                throw std::runtime_error("duplicate detected while tarring segment");
        }

        // Remove the .metadata file if present, because we are shuffling the
        // data file and it will not be valid anymore
        string mdpathname = segment->segment().abspath + ".metadata";
        if (sys::exists(mdpathname))
            if (unlink(mdpathname.c_str()) < 0)
            {
                stringstream ss;
                ss << "cannot remove obsolete metadata file " << mdpathname;
                throw std::system_error(errno, std::system_category(), ss.str());
            }

        // Commit the changes in the database
        p.commit();
    }

    void zip() override
    {
        if (sys::exists(segment->segment().abspath + ".zip"))
            return;

        auto write_lock = lock->write_lock();
        Pending p = idx().begin_transaction();

        // Rescan file
        metadata::Collection mds;
        idx().scan(checker.segment_manager(), mds.inserter_func(), "reftime, offset");

        // Create the .tar segment
        segment = segment->zip(mds);

        // Reindex the new metadata
        idx().reset();
        for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
        {
            const source::Blob& source = (*i)->sourceBlob();
            if (idx().index(**i, source.offset))
                throw std::runtime_error("duplicate detected while zipping segment");
        }

        // Remove the .metadata file if present, because we are shuffling the
        // data file and it will not be valid anymore
        string mdpathname = segment->segment().abspath + ".metadata";
        if (sys::exists(mdpathname))
            if (unlink(mdpathname.c_str()) < 0)
            {
                stringstream ss;
                ss << "cannot remove obsolete metadata file " << mdpathname;
                throw std::system_error(errno, std::system_category(), ss.str());
            }

        // Commit the changes in the database
        p.commit();
    }

    size_t compress(unsigned groupsize) override
    {
        if (sys::exists(segment->segment().abspath + ".gz") || sys::exists(segment->segment().abspath + ".gz.idx"))
            return 0;

        auto write_lock = lock->write_lock();
        Pending p = idx().begin_transaction();

        // Rescan file
        metadata::Collection mds;
        idx().scan(checker.segment_manager(), mds.inserter_func(), "reftime, offset");

        // Create the .tar segment
        size_t old_size = segment->size();
        segment = segment->compress(mds, groupsize);
        size_t new_size = segment->size();

        // Reindex the new metadata
        idx().reset();
        for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
        {
            const source::Blob& source = (*i)->sourceBlob();
            if (idx().index(**i, source.offset))
                throw std::runtime_error("duplicate detected while compressing segment");
        }

        // Remove the .metadata file if present, because we are shuffling the
        // data file and it will not be valid anymore
        string mdpathname = segment->segment().abspath + ".metadata";
        if (sys::exists(mdpathname))
            if (unlink(mdpathname.c_str()) < 0)
            {
                stringstream ss;
                ss << "cannot remove obsolete metadata file " << mdpathname;
                throw std::system_error(errno, std::system_category(), ss.str());
            }

        // Commit the changes in the database
        p.commit();

        if (old_size > new_size)
            return old_size - new_size;
        else
            return 0;
    }

    size_t repack(unsigned test_flags=0) override
    {
        // Lock away writes and reads
        auto write_lock = lock->write_lock();
        Pending p = idx().begin_transaction();

        metadata::Collection mds;
        idx().scan(checker.segment_manager(), mds.inserter_func(), "reftime, offset");

        auto res = reorder_segment_backend(p, mds, test_flags);

        //reporter.operation_progress(checker.name(), "repack", "running VACUUM ANALYZE on segment " + segment->relpath);
        idx().vacuum();

        return res;
    }

    size_t reorder(metadata::Collection& mds, unsigned test_flags) override
    {
        // Lock away writes and reads
        auto write_lock = lock->write_lock();
        Pending p = idx().begin_transaction();

        return reorder_segment_backend(p, mds, test_flags);
    }

    size_t reorder_segment_backend(Pending& p, metadata::Collection& mds, unsigned test_flags)
    {
        // Make a copy of the file with the right data in it, sorted by
        // reftime, and update the offsets in the index
        segment::RepackConfig repack_config;
        repack_config.gz_group_size = config().gz_group_size;
        repack_config.test_flags = test_flags;
        Pending p_repack = segment->repack(checker.config().path, mds, repack_config);

        // Reindex mds
        idx().reset();
        for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
        {
            const source::Blob& source = (*i)->sourceBlob();
            if (idx().index(**i, source.offset))
                throw std::runtime_error("duplicate detected while reordering segment");
        }

        size_t size_pre = sys::isdir(segment->segment().abspath) ? 0 : sys::size(segment->segment().abspath);

        // Remove the .metadata file if present, because we are shuffling the
        // data file and it will not be valid anymore
        string mdpathname = segment->segment().abspath + ".metadata";
        if (sys::exists(mdpathname))
            if (unlink(mdpathname.c_str()) < 0)
            {
                stringstream ss;
                ss << "cannot remove obsolete metadata file " << mdpathname;
                throw std::system_error(errno, std::system_category(), ss.str());
            }

        // Commit the changes in the file system
        p_repack.commit();

        // Commit the changes in the database
        p.commit();

        size_t size_post = sys::isdir(segment->segment().abspath) ? 0 : sys::size(segment->segment().abspath);

        return size_pre - size_post;
    }

    size_t remove(bool with_data) override
    {
        string idx_abspath = str::joinpath(segment->segment().abspath + ".index");
        size_t res = 0;
        if (sys::exists(idx_abspath))
        {
            res = sys::size(idx_abspath);
            sys::unlink(idx_abspath);
        }
        if (!with_data) return res;
        return res + segment->remove();
    }

    void index(metadata::Collection&& mds) override
    {
        // Add to index
        auto write_lock = lock->write_lock();
        Pending p_idx = idx().begin_transaction();
        for (auto& md: mds)
            if (idx().index(*md, md->sourceBlob().offset))
                throw std::runtime_error("duplicate detected while reordering segment");
        p_idx.commit();

        // Remove .metadata and .summary files
        sys::unlink_ifexists(segment->segment().abspath + ".metadata");
        sys::unlink_ifexists(segment->segment().abspath + ".summary");
    }

    void rescan(dataset::Reporter& reporter) override
    {
        metadata::Collection mds;
        segment->rescan_data(
                [&](const std::string& msg) { reporter.segment_info(checker.name(), segment->segment().relpath, msg); },
                lock, mds.inserter_func());

        // Lock away writes and reads
        auto write_lock = lock->write_lock();
        Pending p = idx().begin_transaction();

        // Remove from the index all data about the file
        idx().reset();

        // Scan the list of metadata, looking for duplicates and marking all
        // the duplicates except the last one as deleted
        IDMaker id_maker(idx().unique_codes());

        map<vector<uint8_t>, const Metadata*> finddupes;
        for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
        {
            vector<uint8_t> id = id_maker.make_string(**i);
            if (id.empty())
                continue;
            auto dup = finddupes.find(id);
            if (dup == finddupes.end())
                finddupes.insert(make_pair(id, *i));
            else
                dup->second = *i;
        }
        // cerr << " DUPECHECKED " << pathname << ": " << finddupes.size() << endl;

        // Send the remaining metadata to the reindexer
        std::string basename = str::basename(segment->segment().relpath);
        for (const auto& i: finddupes)
        {
            const Metadata& md = *i.second;
            const source::Blob& blob = md.sourceBlob();
            try {
                if (str::basename(blob.filename) != basename)
                    throw std::runtime_error("cannot rescan " + segment->segment().relpath + ": metadata points to the wrong file: " + blob.filename);
                if (std::unique_ptr<types::source::Blob> old = idx().index(md, blob.offset))
                {
                    stringstream ss;
                    ss << "cannot reindex " << basename << ": data item at offset " << blob.offset << " has a duplicate at offset " << old->offset << ": manual fix is required";
                    throw runtime_error(ss.str());
                }
            } catch (std::exception& e) {
                stringstream ss;
                ss << "cannot reindex " << basename << ": failed to reindex data item at offset " << blob.offset << ": " << e.what();
                throw runtime_error(ss.str());
                // sqlite will take care of transaction consistency
            }
        }
        // cerr << " REINDEXED " << pathname << endl;

        // TODO: if scan fails, remove all info from the index and rename the
        // file to something like .broken

        // Commit the changes on the database
        p.commit();
        // cerr << " COMMITTED" << endl;

        // TODO: remove relevant summary
    }

    void release(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath) override
    {
        sys::unlink_ifexists(str::joinpath(checker.config().path, segment->segment().relpath + ".index"));
        segment = segment->move(new_root, new_relpath, new_abspath);
    }
};


Checker::Checker(std::shared_ptr<const iseg::Config> config)
    : m_config(config)
{
    // Create the directory if it does not exist
    sys::makedirs(config->path);
}

std::string Checker::type() const { return "iseg"; }

void Checker::list_segments(std::function<void(const std::string& relpath)> dest)
{
    list_segments(Matcher(), dest);
}

void Checker::list_segments(const Matcher& matcher, std::function<void(const std::string& relpath)> dest)
{
    vector<string> seg_relpaths;
    config().step().list_segments(config().path, config().format + ".index", matcher, [&](std::string&& s) {
        s.resize(s.size() - 6);
        seg_relpaths.emplace_back(move(s));
    });
    std::sort(seg_relpaths.begin(), seg_relpaths.end());
    for (const auto& relpath: seg_relpaths)
        dest(relpath);
}

std::unique_ptr<segmented::CheckerSegment> Checker::segment(const std::string& relpath)
{
    return unique_ptr<segmented::CheckerSegment>(new CheckerSegment(*this, relpath));
}

std::unique_ptr<segmented::CheckerSegment> Checker::segment_prelocked(const std::string& relpath, std::shared_ptr<dataset::CheckLock> lock)
{
    return unique_ptr<segmented::CheckerSegment>(new CheckerSegment(*this, relpath, lock));
}

void Checker::segments_tracked(std::function<void(segmented::CheckerSegment& segment)> dest)
{
    segments_tracked_filtered(Matcher(), dest);
}

void Checker::segments_tracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)> dest)
{
    list_segments(matcher, [&](const std::string& relpath) {
        CheckerSegment segment(*this, relpath);
        dest(segment);
    });
}

void Checker::segments_untracked(std::function<void(segmented::CheckerSegment& relpath)> dest)
{
    segments_untracked_filtered(Matcher(), dest);
}

void Checker::segments_untracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)> dest)
{
    config().step().list_segments(config().path, config().format, matcher, [&](std::string&& relpath) {
        if (sys::stat(str::joinpath(config().path, relpath + ".index"))) return;
        CheckerSegment segment(*this, relpath);
        dest(segment);
    });
}

void Checker::check_issue51(CheckerConfig& opts)
{
    if (!opts.online && !config().offline) return;
    if (!opts.offline && config().offline) return;

    // Broken metadata for each segment
    std::map<string, metadata::Collection> broken_mds;

    // Iterate all segments
    list_segments([&](const std::string& relpath) {
        auto lock = config().check_lock_segment(relpath);
        CIndex idx(m_config, relpath, lock);
        metadata::Collection mds;
        idx.scan(segment_manager(), mds.inserter_func(), "reftime, offset");
        if (mds.empty()) return;
        File datafile(str::joinpath(config().path, relpath), O_RDONLY);
        // Iterate all metadata in the segment
        unsigned count_otherformat = 0;
        unsigned count_ok = 0;
        unsigned count_issue51 = 0;
        unsigned count_corrupted = 0;
        for (const auto& md: mds) {
            const auto& blob = md->sourceBlob();
            // Keep only segments with grib or bufr files
            if (blob.format != "grib" && blob.format != "bufr")
            {
                ++count_otherformat;
                continue;
            }
            // Read the last 4 characters
            char tail[4];
            if (datafile.pread(tail, 4, blob.offset + blob.size - 4) != 4)
            {
                opts.reporter->segment_info(name(), relpath, "cannot read 4 bytes at position " + std::to_string(blob.offset + blob.size - 4));
                return;
            }
            // Check if it ends with 7777
            if (memcmp(tail, "7777", 4) == 0)
            {
                ++count_ok;
                continue;
            }
            // If it instead ends with 777?, take note of it
            if (memcmp(tail, "777", 3) == 0)
            {
                ++count_issue51;
                broken_mds[relpath].push_back(*md);
            } else {
                ++count_corrupted;
                opts.reporter->segment_info(name(), relpath, "end marker 7777 or 777? not found at position " + std::to_string(blob.offset + blob.size - 4));
            }
        }
        nag::verbose("Checked %s:%s: %u ok, %u other formats, %u issue51, %u corrupted", name().c_str(), relpath.c_str(), count_ok, count_otherformat, count_issue51, count_corrupted);
    });

    if (opts.readonly)
    {
        for (const auto& i: broken_mds)
            opts.reporter->segment_issue51(name(), i.first, "segment contains data with corrupted terminator signature");
    } else {
        for (const auto& i: broken_mds)
        {
            // Make a backup copy with .issue51 extension, if it doesn't already exist
            std::string abspath = str::joinpath(config().path, i.first);
            std::string backup = abspath + ".issue51";
            if (!sys::exists(backup))
            {
                File src(abspath, O_RDONLY);
                File dst(backup, O_WRONLY | O_CREAT | O_EXCL, 0666);
                std::array<char, 40960> buffer;
                while (true)
                {
                    size_t sz = src.read(buffer.data(), buffer.size());
                    if (!sz) break;
                    dst.write_all_or_throw(buffer.data(), sz);
                }
            }

            // Fix the file
            File datafile(abspath, O_RDWR);
            for (const auto& md: i.second) {
                const auto& blob = md->sourceBlob();
                // Keep only segments with grib or bufr files
                if (blob.format != "grib" && blob.format != "bufr")
                    return;
                datafile.pwrite("7", 1, blob.offset + blob.size - 1);
            }
            opts.reporter->segment_issue51(name(), i.first, "fixed corrupted terminator signatures");
        }
    }

    return segmented::Checker::check_issue51(opts);
}

size_t Checker::vacuum(dataset::Reporter& reporter)
{
    return 0;
}

void Checker::test_make_overlap(const std::string& relpath, unsigned overlap_size, unsigned data_idx)
{
    auto lock = config().check_lock_segment(relpath);
    auto wrlock = lock->write_lock();
    CIndex idx(m_config, relpath, lock);
    metadata::Collection mds;
    idx.query_segment(segment_manager(), mds.inserter_func());
    segment_manager().get_checker(config().format, relpath)->test_make_overlap(mds, overlap_size, data_idx);
    idx.test_make_overlap(overlap_size, data_idx);
}

void Checker::test_make_hole(const std::string& relpath, unsigned hole_size, unsigned data_idx)
{
    auto lock = config().check_lock_segment(relpath);
    auto wrlock = lock->write_lock();
    CIndex idx(m_config, relpath, lock);
    metadata::Collection mds;
    idx.query_segment(segment_manager(), mds.inserter_func());
    segment_manager().get_checker(config().format, relpath)->test_make_hole(mds, hole_size, data_idx);
    idx.test_make_hole(hole_size, data_idx);
}

void Checker::test_corrupt_data(const std::string& relpath, unsigned data_idx)
{
    auto lock = config().check_lock_segment(relpath);
    auto wrlock = lock->write_lock();
    CIndex idx(m_config, relpath, lock);
    metadata::Collection mds;
    idx.query_segment(segment_manager(), mds.inserter_func());
    segment_manager().get_checker(config().format, relpath)->test_corrupt(mds, data_idx);
}

void Checker::test_truncate_data(const std::string& relpath, unsigned data_idx)
{
    auto lock = config().check_lock_segment(relpath);
    auto wrlock = lock->write_lock();
    CIndex idx(m_config, relpath, lock);
    metadata::Collection mds;
    idx.query_segment(segment_manager(), mds.inserter_func());
    segment_manager().get_checker(config().format, relpath)->test_truncate(mds, data_idx);
}

void Checker::test_swap_data(const std::string& relpath, unsigned d1_idx, unsigned d2_idx)
{
    auto lock = config().check_lock_segment(relpath);
    metadata::Collection mds;
    {
        CIndex idx(m_config, relpath, lock);
        idx.scan(segment_manager(), mds.inserter_func(), "offset");
        std::swap(mds[d1_idx], mds[d2_idx]);
    }
    segment_prelocked(relpath, lock)->reorder(mds);
}

void Checker::test_rename(const std::string& relpath, const std::string& new_relpath)
{
    auto lock = config().check_lock_segment(relpath);
    auto wrlock = lock->write_lock();
    string abspath = str::joinpath(config().path, relpath);
    string new_abspath = str::joinpath(config().path, new_relpath);
    sys::rename(abspath, new_abspath);
    sys::rename(abspath + ".index", new_abspath + ".index");
}

void Checker::test_change_metadata(const std::string& relpath, Metadata& md, unsigned data_idx)
{
    auto lock = config().check_lock_segment(relpath);
    auto wrlock = lock->write_lock();
    CIndex idx(m_config, relpath, lock);
    metadata::Collection mds;
    idx.query_segment(segment_manager(), mds.inserter_func());
    md.set_source(std::unique_ptr<arki::types::Source>(mds[data_idx].source().clone()));
    md.sourceBlob().unlock();
    mds[data_idx] = md;

    // Reindex mds
    idx.reset();
    for (auto& m: mds)
    {
        const source::Blob& source = m->sourceBlob();
        if (idx.index(*m, source.offset))
            throw std::runtime_error("duplicate detected in test_change_metadata");
    }

    md = mds[data_idx];
}

void Checker::test_remove_index(const std::string& relpath)
{
    sys::unlink_ifexists(str::joinpath(config().path, relpath + ".index"));
}

}
}
}

