#ifndef ARKI_DATASET_INDEXED_H
#define ARKI_DATASET_INDEXED_H

#include <arki/dataset/segmented.h>

namespace arki {
namespace dataset {
class Index;

/// SegmentedReader that can make use of an index
class IndexedReader : public SegmentedReader
{
protected:
    Index* m_idx = nullptr;

public:
    IndexedReader(const ConfigFile& cfg);
    ~IndexedReader();

    void query_data(const dataset::DataQuery& q, metadata_dest_func dest) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
    size_t produce_nth(metadata_dest_func cons, size_t idx=0) override;

    /**
     * Return true if this dataset has a working index.
     *
     * This method is mostly used for tests.
     */
    bool hasWorkingIndex() const { return m_idx != 0; }
};

class IndexedWriter : public SegmentedWriter
{
protected:
    Index* m_idx = nullptr;

public:
    IndexedWriter(const ConfigFile& cfg);
    ~IndexedWriter();
};

}
}
#endif
