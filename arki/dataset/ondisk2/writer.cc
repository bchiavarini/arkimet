#include "arki/dataset/ondisk2/writer.h"
#include "arki/exceptions.h"
#include "arki/binary.h"
#include "arki/dataset/maintenance.h"
#include "arki/dataset/archive.h"
#include "arki/dataset/segment.h"
#include "arki/dataset/reporter.h"
#include "arki/dataset/step.h"
#include "arki/dataset/lock.h"
#include "arki/reader.h"
#include "arki/types/source/blob.h"
#include "arki/types/reftime.h"
#include "arki/configfile.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/matcher.h"
#include "arki/scan/any.h"
#include "arki/utils.h"
#include "arki/utils/files.h"
#include "arki/utils/compress.h"
#include "arki/summary.h"
#include "arki/nag.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <system_error>
#include <sstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cassert>

using namespace std;
using namespace arki::utils;
using namespace arki::types;
using arki::core::Time;

namespace arki {
namespace dataset {
namespace ondisk2 {

struct AppendSegment
{
    std::shared_ptr<const ondisk2::Config> config;
    std::shared_ptr<dataset::AppendLock> lock;
    index::WIndex idx;
    std::shared_ptr<segment::Writer> segment;

    AppendSegment(std::shared_ptr<const ondisk2::Config> config, std::shared_ptr<dataset::AppendLock> lock, std::shared_ptr<segment::Writer> segment)
        : config(config), lock(lock), idx(config), segment(segment)
    {
        idx.lock = lock;
        idx.open();
    }
    ~AppendSegment()
    {
        idx.flush();
    }

    WriterAcquireResult acquire_replace_never(Metadata& md)
    {
        Pending p_idx = idx.begin_transaction();
        try {
            if (std::unique_ptr<types::source::Blob> old = idx.index(md, segment->relname, segment->next_offset()))
            {
                md.add_note("Failed to store in dataset '" + config->name + "' because the dataset already has the data in " + old->filename + ":" + std::to_string(old->offset));
                return ACQ_ERROR_DUPLICATE;
            }
            segment->append(md);
            segment->commit();
            p_idx.commit();
            return ACQ_OK;
        } catch (std::exception& e) {
            // sqlite will take care of transaction consistency
            md.add_note("Failed to store in dataset '" + config->name + "': " + e.what());
            return ACQ_ERROR;
        }
    }

    WriterAcquireResult acquire_replace_always(Metadata& md)
    {
        Pending p_idx = idx.begin_transaction();
        try {
            idx.replace(md, segment->relname, segment->next_offset());
            segment->append(md);
            // In a replace, we necessarily replace inside the same file,
            // as it depends on the metadata reftime
            //createPackFlagfile(df->pathname);
            segment->commit();
            p_idx.commit();
            return ACQ_OK;
        } catch (std::exception& e) {
            // sqlite will take care of transaction consistency
            md.add_note("Failed to store in dataset '" + config->name + "': " + e.what());
            return ACQ_ERROR;
        }
    }

    WriterAcquireResult acquire_replace_higher_usn(Metadata& md)
    {
        Pending p_idx = idx.begin_transaction();

        try {
            // Try to acquire without replacing
            if (std::unique_ptr<types::source::Blob> old = idx.index(md, segment->relname, segment->next_offset()))
            {
                // Duplicate detected

                // Read the update sequence number of the new BUFR
                int new_usn;
                if (!scan::update_sequence_number(md, new_usn))
                    return ACQ_ERROR_DUPLICATE;

                // Read the update sequence number of the old BUFR
                auto reader = arki::Reader::create_new(old->absolutePathname(), lock);
                old->lock(reader);
                int old_usn;
                if (!scan::update_sequence_number(*old, old_usn))
                {
                    md.add_note("Failed to store in dataset '" + config->name + "': a similar element exists, the new element has an Update Sequence Number but the old one does not, so they cannot be compared");
                    return ACQ_ERROR;
                }

                // If the new element has no higher Update Sequence Number, report a duplicate
                if (old_usn > new_usn)
                    return ACQ_ERROR_DUPLICATE;

                // Replace, reusing the pending datafile transaction from earlier
                idx.replace(md, segment->relname, segment->next_offset());
                segment->append(md);
                segment->commit();
                p_idx.commit();
                return ACQ_OK;
            } else {
                segment->append(md);
                segment->commit();
                p_idx.commit();
                return ACQ_OK;
            }
        } catch (std::exception& e) {
            // sqlite will take care of transaction consistency
            md.add_note("Failed to store in dataset '" + config->name + "': " + e.what());
            return ACQ_ERROR;
        }
    }


    void acquire_batch_replace_never(std::vector<std::shared_ptr<WriterBatchElement>>& batch)
    {
        Pending p_idx = idx.begin_transaction();

        for (auto& e: batch)
        {
            e->dataset_name.clear();

            if (std::unique_ptr<types::source::Blob> old = idx.index(e->md, segment->relname, segment->next_offset()))
            {
                e->md.add_note("Failed to store in dataset '" + config->name + "' because the dataset already has the data in " + old->filename + ":" + std::to_string(old->offset));
                e->result = ACQ_ERROR_DUPLICATE;
                continue;
            }
            segment->append(e->md);
            e->result = ACQ_OK;
            e->dataset_name = config->name;
        }

        segment->commit();
        p_idx.commit();
    }

    void acquire_batch_replace_always(std::vector<std::shared_ptr<WriterBatchElement>>& batch)
    {
        Pending p_idx = idx.begin_transaction();

        for (auto& e: batch)
        {
            e->dataset_name.clear();
            idx.replace(e->md, segment->relname, segment->next_offset());
            segment->append(e->md);
            e->result = ACQ_OK;
            e->dataset_name = config->name;
        }

        segment->commit();
        p_idx.commit();
    }

    void acquire_batch_replace_higher_usn(std::vector<std::shared_ptr<WriterBatchElement>>& batch)
    {
        Pending p_idx = idx.begin_transaction();

        for (auto& e: batch)
        {
            e->dataset_name.clear();

            // Try to acquire without replacing
            if (std::unique_ptr<types::source::Blob> old = idx.index(e->md, segment->relname, segment->next_offset()))
            {
                // Duplicate detected

                // Read the update sequence number of the new BUFR
                int new_usn;
                if (!scan::update_sequence_number(e->md, new_usn))
                {
                    e->md.add_note("Failed to store in dataset '" + config->name + "' because the dataset already has the data in " + segment->relname + ":" + std::to_string(old->offset) + " and there is no Update Sequence Number to compare");
                    e->result = ACQ_ERROR_DUPLICATE;
                    continue;
                }

                // Read the update sequence number of the old BUFR
                auto reader = arki::Reader::create_new(old->absolutePathname(), lock);
                old->lock(reader);
                int old_usn;
                if (!scan::update_sequence_number(*old, old_usn))
                {
                    e->md.add_note("Failed to store in dataset '" + config->name + "': a similar element exists, the new element has an Update Sequence Number but the old one does not, so they cannot be compared");
                    e->result = ACQ_ERROR;
                    continue;
                }

                // If the new element has no higher Update Sequence Number, report a duplicate
                if (old_usn > new_usn)
                {
                    e->md.add_note("Failed to store in dataset '" + config->name + "' because the dataset already has the data in " + segment->relname + ":" + std::to_string(old->offset) + " with a higher Update Sequence Number");
                    e->result = ACQ_ERROR_DUPLICATE;
                    continue;
                }

                // Replace, reusing the pending datafile transaction from earlier
                idx.replace(e->md, segment->relname, segment->next_offset());
                segment->append(e->md);
                e->result = ACQ_OK;
                e->dataset_name = config->name;
                continue;
            } else {
                segment->append(e->md);
                e->result = ACQ_OK;
                e->dataset_name = config->name;
            }
        }

        segment->commit();
        p_idx.commit();
    }
};

Writer::Writer(std::shared_ptr<const ondisk2::Config> config)
    : m_config(config)
{
    // Create the directory if it does not exist
    bool dir_created = sys::makedirs(config->path);

    // If the index is missing, take note not to perform a repack until a
    // check is made
    if (!dir_created and !sys::exists(config->index_pathname))
        files::createDontpackFlagfile(config->path);
}

Writer::~Writer()
{
    flush();
}

std::string Writer::type() const { return "ondisk2"; }

std::unique_ptr<AppendSegment> Writer::segment(const Metadata& md, const std::string& format)
{
    auto lock = config().append_lock_dataset();
    return std::unique_ptr<AppendSegment>(new AppendSegment(m_config, lock, segmented::Writer::file(md, format)));
}

std::unique_ptr<AppendSegment> Writer::segment(const std::string& relname)
{
    sys::makedirs(str::dirname(str::joinpath(config().path, relname)));
    auto lock = config().append_lock_dataset();
    auto segment = segment_manager().get_writer(relname);
    return std::unique_ptr<AppendSegment>(new AppendSegment(m_config, lock, segment));
}

WriterAcquireResult Writer::acquire(Metadata& md, ReplaceStrategy replace)
{
    auto age_check = config().check_acquire_age(md);
    if (age_check.first) return age_check.second;

    if (replace == REPLACE_DEFAULT) replace = config().default_replace_strategy;

    auto w = segment(md, md.source().format);

    switch (replace)
    {
        case REPLACE_NEVER: return w->acquire_replace_never(md);
        case REPLACE_ALWAYS: return w->acquire_replace_always(md);
        case REPLACE_HIGHER_USN: return w->acquire_replace_higher_usn(md);
        default:
        {
            stringstream ss;
            ss << "cannot acquire into dataset " << name() << ": replace strategy " << (int)replace << " is not supported";
            throw runtime_error(ss.str());
        }
    }
}

void Writer::acquire_batch(std::vector<std::shared_ptr<WriterBatchElement>>& batch, ReplaceStrategy replace)
{
    if (replace == REPLACE_DEFAULT) replace = config().default_replace_strategy;

    // Clear dataset names, pre-process items that do not need further
    // dispatching, and divide the rest of the batch by segment
    std::map<std::string, std::vector<std::shared_ptr<dataset::WriterBatchElement>>> by_segment;
    for (auto& e: batch)
    {
        e->dataset_name.clear();

        switch (replace)
        {
            case REPLACE_NEVER:
            case REPLACE_ALWAYS:
            case REPLACE_HIGHER_USN: break;
            default:
            {
                e->md.add_note("cannot acquire into dataset " + name() + ": replace strategy " + std::to_string(replace) + " is not supported");
                e->result = ACQ_ERROR;
                continue;
            }
        }

        auto age_check = config().check_acquire_age(e->md);
        if (age_check.first)
        {
            e->result = age_check.second;
            if (age_check.second == ACQ_OK)
                e->dataset_name = name();
            continue;
        }

        const core::Time& time = e->md.get<types::reftime::Position>()->time;
        string relname = config().step()(time) + "." + e->md.source().format;
        by_segment[relname].push_back(e);
    }

    // Import segment by segment
    for (auto& s: by_segment)
    {
        auto seg = segment(s.first);
        switch (replace)
        {
            case REPLACE_NEVER:
                seg->acquire_batch_replace_never(s.second);
                break;
            case REPLACE_ALWAYS:
                seg->acquire_batch_replace_always(s.second);
                break;
            case REPLACE_HIGHER_USN:
                seg->acquire_batch_replace_higher_usn(s.second);
                break;
            default: throw std::runtime_error("programmign error: replace value has changed since previous check");
        }
    }
}

void Writer::remove(Metadata& md)
{
    const types::source::Blob* source = md.has_source_blob();
    if (!source)
        throw std::runtime_error("cannot remove metadata from dataset, because it has no Blob source");

    if (source->basedir != config().path)
        throw std::runtime_error("cannot remove metadata from dataset: its basedir is " + source->basedir + " but this dataset is at " + config().path);

    // TODO: refuse if md is in the archive

    auto lock = config().append_lock_dataset();

    // Delete from DB, and get file name
    index::WIndex idx(m_config);
    idx.open();
    idx.lock = lock;
    Pending p_del = idx.begin_transaction();
    idx.remove(source->filename, source->offset);

    // Create flagfile
    //createPackFlagfile(str::joinpath(config().path, file));

    // Commit delete from DB
    p_del.commit();

    // reset source and dataset in the metadata
    md.unset_source();
    md.unset(TYPE_ASSIGNEDDATASET);
}

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

    CheckerSegment(Checker& checker, const std::string& relpath, std::shared_ptr<dataset::CheckLock> lock)
        : segmented::CheckerSegment(lock), checker(checker)
    {
        segment = checker.segment_manager().get_checker(relpath);
    }

    std::string path_relative() const override { return segment->relname; }
    const ondisk2::Config& config() const override { return checker.config(); }
    dataset::ArchivesChecker& archives() const { return checker.archive(); }

    segmented::SegmentState scan(dataset::Reporter& reporter, bool quick=true) override
    {
        if (!segment->exists_on_disk())
            return segmented::SegmentState(SEGMENT_MISSING);

        if (!checker.m_idx->has_segment(segment->relname))
        {
            bool untrusted_index = files::hasDontpackFlagfile(checker.config().path);
            reporter.segment_info(checker.name(), segment->relname, "segment found on disk with no associated index data");
            return segmented::SegmentState(untrusted_index ? SEGMENT_UNALIGNED : SEGMENT_DELETED);
        }

        metadata::Collection mds;
        checker.idx->scan_file(segment->relname, mds.inserter_func(), "m.file, m.reftime, m.offset");

        segment::State state = SEGMENT_OK;
        bool untrusted_index = files::hasDontpackFlagfile(checker.config().path);

        // Compute the span of reftimes inside the segment
        unique_ptr<Time> md_begin;
        unique_ptr<Time> md_until;
        if (mds.empty())
        {
            reporter.segment_info(checker.name(), segment->relname, "index knows of this segment but contains no data for it");
            md_begin.reset(new Time(0, 0, 0));
            md_until.reset(new Time(0, 0, 0));
            state = untrusted_index ? SEGMENT_UNALIGNED : SEGMENT_DELETED;
        } else {
            if (!mds.expand_date_range(md_begin, md_until))
            {
                reporter.segment_info(checker.name(), segment->relname, "index data for this segment has no reference time information");
                state = SEGMENT_CORRUPTED;
                md_begin.reset(new Time(0, 0, 0));
                md_until.reset(new Time(0, 0, 0));
            } else {
                // Ensure that the reftime span fits inside the segment step
                Time seg_begin;
                Time seg_until;
                if (checker.config().step().path_timespan(segment->relname, seg_begin, seg_until))
                {
                    if (*md_begin < seg_begin || *md_until > seg_until)
                    {
                        reporter.segment_info(checker.name(), segment->relname, "segment contents do not fit inside the step of this dataset");
                        state = SEGMENT_CORRUPTED;
                    }
                    // Expand segment timespan to the full possible segment timespan
                    *md_begin = seg_begin;
                    *md_until = seg_until;
                } else {
                    reporter.segment_info(checker.name(), segment->relname, "segment name does not fit the step of this dataset");
                    state = SEGMENT_CORRUPTED;
                }
            }
        }

        if (!checker.segment_manager().exists(segment->relname))
        {
            // The segment does not exist on disk
            reporter.segment_info(checker.name(), segment->relname, "segment found in index but not on disk");
            state = state - SEGMENT_UNALIGNED + SEGMENT_MISSING;
        }

        if (state.is_ok())
            state = segment->check(reporter, checker.name(), mds, quick);

        // Scenario: the index has been deleted, and some data has been imported
        // and appended to an existing segment, recreating an empty index.
        // That segment would show in the index as DIRTY, because it has a gap of
        // data not indexed. Since the needs-check-do-not-pack file is present,
        // however, mark that file for rescanning instead of repacking.
        if (untrusted_index && state.has(SEGMENT_DIRTY))
            state = state - SEGMENT_DIRTY + SEGMENT_UNALIGNED;

        auto res = segmented::SegmentState(state, *md_begin, *md_until);
        res.check_age(segment->relname, checker.config(), reporter);
        return res;
    }

    size_t repack(unsigned test_flags) override
    {
        auto lock = checker.lock->write_lock();
        metadata::Collection mds;
        checker.idx->scan_file(segment->relname, mds.inserter_func(), "reftime, offset");
        return reorder(mds, test_flags);
    }

    size_t reorder(metadata::Collection& mds, unsigned test_flags) override
    {
        // Lock away writes and reads
        Pending p = checker.idx->begin_transaction();

        Pending p_repack = segment->repack(checker.config().path, mds, test_flags);

        // Reindex mds
        checker.idx->reset(segment->relname);
        for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
        {
            const source::Blob& source = (*i)->sourceBlob();
            checker.idx->index(**i, source.filename, source.offset);
        }

        size_t size_pre = sys::isdir(segment->absname) ? 0 : sys::size(segment->absname);

        // Remove the .metadata file if present, because we are shuffling the
        // data file and it will not be valid anymore
        string mdpathname = segment->absname + ".metadata";
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

        size_t size_post = sys::isdir(segment->absname) ? 0 : sys::size(segment->absname);

        return size_pre - size_post;
    }

    size_t remove(bool with_data) override
    {
        checker.idx->reset(segment->relname);
        // TODO: also remove .metadata and .summary files
        if (!with_data) return 0;
        return segment->remove();
    }

    void rescan() override
    {
        // Temporarily uncompress the file for scanning
        unique_ptr<utils::compress::TempUnzip> tu;
        if (scan::isCompressed(segment->absname))
            tu.reset(new utils::compress::TempUnzip(segment->absname));

        // Collect the scan results in a metadata::Collector
        metadata::Collection mds;
        if (!scan::scan(segment->absname, lock, mds.inserter_func()))
            throw std::runtime_error("cannot rescan " + segment->absname + ": file format unknown");
        //fprintf(stderr, "SCANNED %s: %zd\n", pathname.c_str(), mds.size());

        // Lock away writes and reads
        Pending p = checker.idx->begin_transaction();
        // cerr << "LOCK" << endl;

        // Remove from the index all data about the file
        checker.idx->reset(segment->relname);
        // cerr << " RESET " << file << endl;

        // Scan the list of metadata, looking for duplicates and marking all
        // the duplicates except the last one as deleted
        IDMaker id_maker(checker.idx->unique_codes());

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
        std::string basename = str::basename(segment->relname);
        for (const auto& i: finddupes)
        {
            const Metadata& md = *i.second;
            const source::Blob& blob = md.sourceBlob();
            try {
                if (str::basename(blob.filename) != basename)
                    throw std::runtime_error("cannot rescan " + segment->relname + ": metadata points to the wrong file: " + blob.filename);
                if (std::unique_ptr<types::source::Blob> old = checker.idx->index(md, segment->relname, blob.offset))
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

    void index(metadata::Collection&& contents) override
    {
        // Add to index
        Pending p_idx = checker.idx->begin_transaction();
        for (auto& md: contents)
            if (checker.idx->index(*md, segment->relname, md->sourceBlob().offset))
                throw std::runtime_error("duplicate detected while reordering segment");
        p_idx.commit();

        // Remove .metadata and .summary files
        sys::unlink_ifexists(segment->absname + ".metadata");
        sys::unlink_ifexists(segment->absname + ".summary");
    }

    void release(const std::string& destpath) override
    {
        // Rebuild the metadata
        metadata::Collection mds;
        checker.idx->scan_file(segment->relname, mds.inserter_func());
        mds.writeAtomically(segment->absname + ".metadata");

        // Remove from index
        checker.idx->reset(segment->relname);

        segmented::CheckerSegment::release(destpath);
    }
};


Checker::Checker(std::shared_ptr<const ondisk2::Config> config)
    : m_config(config), idx(new index::WIndex(config))
{
    m_idx = idx;

    // Create the directory if it does not exist
    bool dir_created = sys::makedirs(config->path);

    lock = config->check_lock_dataset();
    m_idx->lock = lock;

    // If the index is missing, take note not to perform a repack until a
    // check is made
    if (!dir_created && !sys::exists(config->index_pathname))
        files::createDontpackFlagfile(config->path);

    idx->open();
}

Checker::~Checker()
{
}

std::string Checker::type() const { return "ondisk2"; }

void Checker::remove_all(dataset::Reporter& reporter, bool writable)
{
    IndexedChecker::remove_all(reporter, writable);
}

void Checker::remove_all_filtered(const Matcher& matcher, dataset::Reporter& reporter, bool writable)
{
    IndexedChecker::remove_all_filtered(matcher, reporter, writable);
}

void Checker::repack(dataset::Reporter& reporter, bool writable, unsigned test_flags)
{
    IndexedChecker::repack(reporter, writable, test_flags);
}

void Checker::repack_filtered(const Matcher& matcher, dataset::Reporter& reporter, bool writable, unsigned test_flags)
{
    IndexedChecker::repack_filtered(matcher, reporter, writable, test_flags);
}

void Checker::check(dataset::Reporter& reporter, bool fix, bool quick)
{
    // TODO: escalate to a write lock while repacking a file
    IndexedChecker::check(reporter, fix, quick);

    if (!idx->checkSummaryCache(*this, reporter) && fix)
    {
        reporter.operation_progress(name(), "check", "rebuilding summary cache");
        idx->rebuildSummaryCache();
    }
}

void Checker::check_filtered(const Matcher& matcher, dataset::Reporter& reporter, bool fix, bool quick)
{
    IndexedChecker::check_filtered(matcher, reporter, fix, quick);

    if (!idx->checkSummaryCache(*this, reporter) && fix)
    {
        reporter.operation_progress(name(), "check", "rebuilding summary cache");
        idx->rebuildSummaryCache();
    }
}

std::unique_ptr<segmented::CheckerSegment> Checker::segment(const std::string& relpath)
{
    return unique_ptr<segmented::CheckerSegment>(new CheckerSegment(*this, relpath, lock));
}

std::unique_ptr<segmented::CheckerSegment> Checker::segment_prelocked(const std::string& relpath, std::shared_ptr<dataset::CheckLock> lock)
{
    return unique_ptr<segmented::CheckerSegment>(new CheckerSegment(*this, relpath, lock));
}

void Checker::segments(std::function<void(segmented::CheckerSegment& segment)> dest)
{
    m_idx->list_segments([&](const std::string& relpath) {
        CheckerSegment segment(*this, relpath, lock);
        dest(segment);
    });
}

void Checker::segments_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)> dest)
{
    m_idx->list_segments_filtered(matcher, [&](const std::string& relpath) {
        CheckerSegment segment(*this, relpath, lock);
        dest(segment);
    });
}

void Checker::segments_untracked(std::function<void(segmented::CheckerSegment& relpath)> dest)
{
    segment_manager().scan_dir([&](const std::string& relpath) {
        if (m_idx->has_segment(relpath)) return;
        CheckerSegment segment(*this, relpath, lock);
        dest(segment);
    });
}

void Checker::segments_untracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& relpath)> dest)
{
    if (matcher.empty()) return segments_untracked(dest);
    auto m = matcher.get(TYPE_REFTIME);
    if (!m) return segments_untracked(dest);

    segment_manager().scan_dir([&](const std::string& relpath) {
        if (m_idx->has_segment(relpath)) return;
        if (!config().step().pathMatches(relpath, *m)) return;
        CheckerSegment segment(*this, relpath, lock);
        dest(segment);
    });
}


size_t Checker::vacuum(dataset::Reporter& reporter)
{
    reporter.operation_progress(name(), "repack", "running VACUUM ANALYZE on the dataset index");
    size_t size_pre = 0, size_post = 0;
    if (sys::size(idx->pathname(), 0) > 0)
    {
        size_pre = sys::size(idx->pathname(), 0)
                 + sys::size(idx->pathname() + "-journal", 0);
        idx->vacuum();
        size_post = sys::size(idx->pathname(), 0)
                  + sys::size(idx->pathname() + "-journal", 0);
    }

    // Rebuild the cached summaries, if needed
    if (!sys::exists(str::joinpath(config().path, ".summaries/all.summary")))
    {
        reporter.operation_progress(name(), "repack", "rebuilding the summary cache");
        Summary s;
        idx->summaryForAll(s);
    }

    return size_pre > size_post ? size_pre - size_post : 0;
}

void Checker::test_change_metadata(const std::string& relpath, Metadata& md, unsigned data_idx)
{
    metadata::Collection mds;
    idx->query_segment(relpath, mds.inserter_func());
    md.set_source(std::unique_ptr<arki::types::Source>(mds[data_idx].source().clone()));
    md.sourceBlob().unlock();
    mds[data_idx] = md;

    // Reindex mds
    idx->reset(relpath);
    for (auto& m: mds)
    {
        const source::Blob& source = m->sourceBlob();
        idx->index(*m, source.filename, source.offset);
    }

    md = mds[data_idx];
}

void Checker::test_remove_index(const std::string& relpath)
{
    m_idx->test_deindex(relpath);
}


void Writer::test_acquire(const ConfigFile& cfg, std::vector<std::shared_ptr<WriterBatchElement>>& batch, std::ostream& out)
{
    ReplaceStrategy replace;
    string repl = cfg.value("replace");
    if (repl == "yes" || repl == "true" || repl == "always")
        replace = REPLACE_ALWAYS;
    else if (repl == "USN")
        replace = REPLACE_HIGHER_USN;
    else if (repl == "" || repl == "no" || repl == "never")
        replace = REPLACE_NEVER;
    else
        throw std::runtime_error("Replace strategy '" + repl + "' is not recognised in dataset configuration");

    // Refuse if md is before "archive age"
    std::shared_ptr<const ondisk2::Config> config(new ondisk2::Config(cfg));
    for (auto& e: batch)
    {
        auto age_check = config->check_acquire_age(e->md);
        if (age_check.first)
        {
            e->result = age_check.second;
            if (age_check.second == ACQ_OK)
                e->dataset_name = config->name;
            else
                e->dataset_name.clear();
            continue;
        }

        if (replace == REPLACE_ALWAYS)
        {
            e->result = ACQ_OK;
            e->dataset_name = config->name;
            continue;
        }

        auto lock = config->read_lock_dataset();

        index::RIndex idx(config);
        if (!idx.exists())
        {
            // If there is no index, there can be no duplicates
            e->result = ACQ_OK;
            e->dataset_name = config->name;
            continue;
        }
        idx.lock = lock;
        idx.open();

        std::unique_ptr<types::source::Blob> old = idx.get_current(e->md);
        if (!old)
        {
            e->result = ACQ_OK;
            e->dataset_name = config->name;
            continue;
        }

        if (replace == REPLACE_NEVER) {
            e->result = ACQ_ERROR_DUPLICATE;
            e->dataset_name.clear();
            continue;
        }

        // We are left with the case of REPLACE_HIGHER_USN

        // Read the update sequence number of the new BUFR
        int new_usn;
        if (!scan::update_sequence_number(e->md, new_usn))
        {
            e->result = ACQ_ERROR_DUPLICATE;
            e->dataset_name.clear();
            continue;
        }

        // Read the update sequence number of the old BUFR
        auto reader = arki::Reader::create_new(old->absolutePathname(), lock);
        old->lock(reader);
        int old_usn;
        if (!scan::update_sequence_number(*old, old_usn))
        {
            out << "cannot acquire into dataset: insert reported a conflict, the new element has an Update Sequence Number but the old one does not, so they cannot be compared";
            e->result = ACQ_ERROR;
            e->dataset_name.clear();
        } else if (old_usn > new_usn) {
            // If there is no new Update Sequence Number, report a duplicate
            e->result = ACQ_ERROR_DUPLICATE;
            e->dataset_name.clear();
        } else {
            e->result = ACQ_OK;
            e->dataset_name = config->name;
        }
    }
}

}
}
}
