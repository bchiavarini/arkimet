#ifndef ARKI_DATASET_SHARD_H
#define ARKI_DATASET_SHARD_H

#include <arki/dataset/local.h>
#include <unordered_map>

namespace arki {
namespace dataset {
class ShardStep;
class Step;

namespace simple { class Config; }
namespace ondisk2 { class Config; }

namespace sharded {

template<typename Base>
struct Config : public Base
{
    bool sharded = false;
    std::shared_ptr<ShardStep> shard_step;

    Config(const ConfigFile& cfg);

    virtual std::shared_ptr<const dataset::Config> create_shard(const core::Time&) const = 0;

    // Iterate all shards, in ascending time order, generating their config
    virtual void all_shards(std::function<void(std::shared_ptr<const dataset::Config>)>) const;

    // Iterate all shards matching a matcher, in ascending time order,
    // generating their config
    virtual void query_shards(const Matcher& matcher, std::function<void(std::shared_ptr<const dataset::Config>)>) const;

protected:
    void to_shard(const std::string& shard_path, std::shared_ptr<Step> step);
};


template<typename Config>
class Reader : public LocalReader
{
protected:
    std::shared_ptr<const Config> m_config;

public:
    Reader(std::shared_ptr<const Config> config);
    ~Reader();

    const Config& config() const override { return *m_config; }

    void query_data(const dataset::DataQuery& q, metadata_dest_func dest) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
    void expand_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end) override;
};

template<typename Config>
class Writer : public LocalWriter
{
protected:
    std::shared_ptr<const Config> m_config;
    std::unordered_map<std::string, dataset::Writer*> shards;

    dataset::Writer& shard(const core::Time& time);

public:
    Writer(std::shared_ptr<const Config> config);
    ~Writer();

    const Config& config() const override { return *m_config; }

    AcquireResult acquire(Metadata& md, ReplaceStrategy replace=REPLACE_DEFAULT) override;
    void remove(Metadata& md) override;
    void flush() override;

};

template<typename Config>
class Checker : public LocalChecker
{
protected:
    std::shared_ptr<const Config> m_config;

public:
    Checker(std::shared_ptr<const Config> config);
    ~Checker();

    const Config& config() const override { return *m_config; }

    void removeAll(dataset::Reporter& reporter, bool writable=false) override;
    void repack(dataset::Reporter& reporter, bool writable=false) override;
    void check(dataset::Reporter& reporter, bool fix, bool quick) override;
};

// gcc bug? If these are uncommented, some template instantiation happens
// before gcc can know that IndexedConfig is a dataset::Config, and as a
// consequence template instantiation fails on covariant return types.
// What I expect should happen is that all template instantiation is delayed
// until the non-extern explicit instantiation is found, and which point the
// whole class hierarchy is known.
//extern template class Config<dataset::IndexedConfig>;
//extern template class Reader<simple::Config>;
////extern template class Reader<ondisk2::Config>;
//extern template class Writer<simple::Config>;
////extern template class Writer<ondisk2::Config>;
//extern template class Checker<simple::Config>;
////extern template class Checker<ondisk2::Config>;

}
}
}
#endif
