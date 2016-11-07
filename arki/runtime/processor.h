#ifndef ARKI_RUNTIME_PROCESSOR_H
#define ARKI_RUNTIME_PROCESSOR_H

/// Run user requested operations on datasets

#include <arki/utils/sys.h>
#include <string>
#include <memory>

namespace arki {
class Matcher;

namespace dataset {
class Reader;
}

namespace runtime {

struct DatasetProcessor
{
    virtual ~DatasetProcessor() {}

    virtual void process(dataset::Reader& ds, const std::string& name) = 0;
    virtual void end() {}
    virtual std::string describe() const = 0;
};

struct SingleOutputProcessor : public DatasetProcessor
{
    utils::sys::NamedFileDescriptor output;

    SingleOutputProcessor();
    SingleOutputProcessor(const utils::sys::NamedFileDescriptor& out);
};

struct ProcessorMaker
{
    bool summary = false;
    bool summary_short = false;
    bool yaml = false;
    bool json = false;
    bool annotate = false;
    bool data_only = false;
    bool data_inline = false;
    // True if we are running in arki-server and we are running the server side
    // of a remote query
    bool server_side = false;
    std::string postprocess;
    std::string report;
    std::string targetfile;

    std::string summary_restrict;
    std::string sort;

    std::function<void(NamedFileDescriptor&)> data_start_hook;

    /// Create the processor maker for this configuration
    std::unique_ptr<DatasetProcessor> make(Matcher query, utils::sys::NamedFileDescriptor& out);

    /**
     * Consistency check on the maker configuration.
     *
     * @returns the empty string if all is ok, else an error message
     */
    std::string verify_option_consistency();
};


}
}
#endif
