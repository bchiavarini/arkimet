#include "config.h"
#include "empty.h"
#include "arki/metadata/collection.h"
#include "arki/utils/accounting.h"
#include <ostream>

using namespace std;

namespace arki {
namespace dataset {
namespace empty {

Config::Config(const core::cfg::Section& cfg)
    : dataset::Config(cfg)
{
}

std::shared_ptr<const Config> Config::create(const core::cfg::Section& cfg)
{
    return std::shared_ptr<const Config>(new Config(cfg));
}

std::unique_ptr<dataset::Reader> Config::create_reader() const { return std::unique_ptr<dataset::Reader>(new Reader(shared_from_this())); }
std::unique_ptr<dataset::Writer> Config::create_writer() const { return std::unique_ptr<dataset::Writer>(new Writer(shared_from_this())); }
std::unique_ptr<dataset::Checker> Config::create_checker() const { return std::unique_ptr<dataset::Checker>(new Checker(shared_from_this())); }


Reader::Reader(std::shared_ptr<const dataset::Config> config) : m_config(config) {}
Reader::~Reader() {}


WriterAcquireResult Writer::acquire(Metadata& md, const AcquireConfig& cfg)
{
    utils::acct::acquire_single_count.incr();
    return ACQ_OK;
}

void Writer::acquire_batch(WriterBatch& batch, const AcquireConfig& cfg)
{
    utils::acct::acquire_batch_count.incr();
    for (auto& e: batch)
    {
        e->result = ACQ_OK;
        e->dataset_name = name();
    }
}

void Writer::test_acquire(const core::cfg::Section& cfg, WriterBatch& batch)
{
    std::shared_ptr<const empty::Config> config(new empty::Config(cfg));
    for (auto& e: batch)
    {
        e->result = ACQ_OK;
        e->dataset_name = config->name;
    }
}

}
}
}
