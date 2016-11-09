#ifndef ARKI_RUNTIME_H
#define ARKI_RUNTIME_H

/// Common code used in most arkimet executables

#include <arki/utils/commandline/parser.h>
#include <arki/runtime/io.h>
#include <arki/runtime/config.h>
#include <arki/runtime/processor.h>
#include <arki/metadata.h>
#include <arki/dataset/memory.h>
#include <arki/matcher.h>
#include <arki/configfile.h>
#include <string>
#include <memory>
#include <vector>

namespace arki {
class Summary;
class Formatter;

namespace runtime {
struct ArkiTool;

/**
 * Initialise the libraries that we use and parse the matcher alias database.
 */
void init();

std::unique_ptr<dataset::Reader> make_qmacro_dataset(const ConfigFile& ds_cfg, const ConfigFile& dispatch_cfg, const std::string& qmacroname, const std::string& query, const std::string& url=std::string());

/**
 * Exception raised when the command line parser has handled the current
 * command invocation.
 *
 * For example, this happens when using --validate=list, which prints a list of
 * validators then exits.
 */
struct HandledByCommandLineParser
{
    /// The exit status that we should return from main
    int status;

    HandledByCommandLineParser(int status=0);
    ~HandledByCommandLineParser();
};

struct CommandLine : public utils::commandline::StandardParserWithManpage
{
    utils::commandline::OptionGroup* infoOpts;
    utils::commandline::OptionGroup* inputOpts;
    utils::commandline::OptionGroup* outputOpts;

    utils::commandline::BoolOption* verbose = nullptr;
    utils::commandline::BoolOption* debug = nullptr;
    utils::commandline::BoolOption* yaml = nullptr;
    utils::commandline::BoolOption* json = nullptr;
    utils::commandline::BoolOption* annotate = nullptr;
    utils::commandline::BoolOption* dataInline = nullptr;
    utils::commandline::BoolOption* dataOnly = nullptr;
    utils::commandline::BoolOption* summary = nullptr;
    utils::commandline::BoolOption* summary_short = nullptr;
    utils::commandline::StringOption* files = nullptr;
    utils::commandline::StringOption* outfile = nullptr;
    utils::commandline::StringOption* targetfile = nullptr;
    utils::commandline::StringOption* postprocess = nullptr;
    utils::commandline::StringOption* report = nullptr;
    utils::commandline::StringOption* sort = nullptr;
    utils::commandline::StringOption* summary_restrict = nullptr;
    utils::commandline::VectorOption<utils::commandline::ExistingFile>* postproc_data = nullptr;
    utils::commandline::VectorOption<utils::commandline::String>* cfgfiles = nullptr;

    std::vector<std::string> input_args;

    CommandLine(const std::string& name, int mansection=1);
    ~CommandLine();

    /**
     * Parse the command line
     */
    bool parse(int argc, const char* argv[]) override;

    virtual void parse_positional_args();

    void parse_all(int argc, const char* argv[]);

    virtual ConfigFile get_inputs();

    void configure(ArkiTool& tool);
};

struct ArkiTool
{
    ConfigFile input_info;
    utils::sys::NamedFileDescriptor* output = nullptr;
    DatasetProcessor* processor = nullptr;
    ProcessorMaker pmaker;
    std::string strquery;
    Matcher query;

    ~ArkiTool();

    virtual void print_config(FILE* out) const;

    /// Set the output file name
    void set_output(const std::string& pathname);

    /// Set the query for filtering output
    virtual void set_query(const std::string& strquery);

    /**
     * End processing and flush partial data
     */
    void doneProcessing();

    /**
     * Open the next data source to process
     *
     * @return the pointer to the datasource, or 0 for no more datasets
     */
    virtual std::unique_ptr<dataset::Reader> open_source(ConfigFile& info);

    /**
     * Process one data source
     *
     * If everything went perfectly well, returns true, else false. It can
     * still throw an exception if things go wrong.
     */
    virtual bool process_source(dataset::Reader& ds, const std::string& name);

    /**
     * Done working with one data source
     *
     * @param successful
     *   true if everything went well, false if there were issues
     * FIXME: put something that contains a status report instead, for
     * FIXME: --status, as well as a boolean for moveok/moveko
     */
    virtual void close_source(std::unique_ptr<dataset::Reader> ds, bool successful=true);

    /// Main body of the tool
    virtual int main();
};

}
}
#endif
