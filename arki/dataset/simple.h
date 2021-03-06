#ifndef ARKI_DATASET_SIMPLE_H
#define ARKI_DATASET_SIMPLE_H

#include <arki/dataset/indexed.h>

namespace arki {
namespace dataset {
namespace simple {

struct Config : public dataset::IndexedConfig
{
    std::string index_type;

    Config(const Config&) = default;
    Config(const core::cfg::Section& cfg);

    std::unique_ptr<dataset::Reader> create_reader() const override;
    std::unique_ptr<dataset::Writer> create_writer() const override;
    std::unique_ptr<dataset::Checker> create_checker() const override;

    static std::shared_ptr<const Config> create(const core::cfg::Section& cfg);
};

}
}
}
#endif
