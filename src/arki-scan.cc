#include "config.h"
#include "arki/utils/commandline/parser.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/matcher.h"
#include "arki/dataset.h"
#include "arki/dataset/file.h"
#include "arki/validator.h"
#include "arki/dispatcher.h"
#include "arki/runtime.h"
#include "arki/nag.h"
#include "config.h"
#include <memory>
#include <iostream>
#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#if __xlC__
// From glibc
#define timersub(a, b, result)                                                \
  do {                                                                        \
    (result)->tv_sec = (a)->tv_sec - (b)->tv_sec;                             \
    (result)->tv_usec = (a)->tv_usec - (b)->tv_usec;                          \
    if ((result)->tv_usec < 0) {                                              \
      --(result)->tv_sec;                                                     \
      (result)->tv_usec += 1000000;                                           \
    }                                                                         \
  } while (0)
#endif



using namespace std;
using namespace arki;
using namespace arki::utils;

namespace {

struct MetadataDispatch;

static std::string moveFile(const std::string& source, const std::string& targetdir)
{
    string targetFile = str::joinpath(targetdir, str::basename(source));
    if (::rename(source.c_str(), targetFile.c_str()) == -1)
        throw_system_error("cannot move " + source + " to " + targetFile);
    return targetFile;
}

static std::string moveFile(const dataset::Reader& ds, const std::string& targetdir)
{
    if (const dataset::File* d = dynamic_cast<const dataset::File*>(&ds))
        return moveFile(d->config().pathname, targetdir);
    else
        return string();
}

/// Dispatch metadata
struct MetadataDispatch
{
    const ConfigFile& cfg;
    Dispatcher* dispatcher = nullptr;
    dataset::Memory results;
    runtime::DatasetProcessor& next;
    bool ignore_duplicates = false;
    bool reportStatus = false;

    // Used for timings. Read with gettimeofday at the beginning of a task,
    // and summarySoFar will report the elapsed time
    struct timeval startTime;

    // Incremented when a metadata is imported in the destination dataset.
    // Feel free to reset it to 0 anytime.
    int countSuccessful = 0;

    // Incremented when a metadata is imported in the error dataset because it
    // had already been imported.  Feel free to reset it to 0 anytime.
    int countDuplicates = 0;

    // Incremented when a metadata is imported in the error dataset.  Feel free
    // to reset it to 0 anytime.
    int countInErrorDataset = 0;

    // Incremented when a metadata is not imported at all.  Feel free to reset
    // it to 0 anytime.
    int countNotImported = 0;

    /// Directory where we store copyok files
    std::string dir_copyok;

    /// Directory where we store copyko files
    std::string dir_copyko;

    /// File to which we send data that was successfully imported
    std::unique_ptr<arki::File> copyok;

    /// File to which we send data that was not successfully imported
    std::unique_ptr<arki::File> copyko;


    MetadataDispatch(const ConfigFile& cfg, runtime::DatasetProcessor& next, bool test=false);
    ~MetadataDispatch();

    /**
     * Dispatch the data from one source
     *
     * @returns true if all went well, false if any problem happend.
     * It can still throw in case of big trouble.
     */
    bool process(dataset::Reader& ds, const std::string& name);

    // Flush all imports done so far
    void flush();

    // Format a summary of the import statistics so far
    std::string summarySoFar() const;

    // Set startTime to the current time
    void setStartTime();

protected:
    bool dispatch(std::unique_ptr<Metadata>&& md);

    void do_copyok(Metadata& md);
    void do_copyko(Metadata& md);
};

struct ScanOptions
{
    utils::commandline::OptionGroup* dispatchOpts = nullptr;
    utils::commandline::BoolOption* ignore_duplicates = nullptr;

    utils::commandline::StringOption* moveok = nullptr;
    utils::commandline::StringOption* moveko = nullptr;
    utils::commandline::StringOption* movework = nullptr;
    utils::commandline::StringOption* copyok = nullptr;
    utils::commandline::StringOption* copyko = nullptr;
    utils::commandline::StringOption* validate = nullptr;
    utils::commandline::BoolOption* status = nullptr;

    utils::commandline::VectorOption<utils::commandline::String>* dispatch = nullptr;
    utils::commandline::VectorOption<utils::commandline::String>* testdispatch = nullptr;

    ConfigFile dispatchInfo;

    void handle_immediate_commands();
    void add_to(runtime::CommandLine& cmd);

    std::unique_ptr<MetadataDispatch> make_dispatcher(runtime::DatasetProcessor& processor);
};

void ScanOptions::add_to(runtime::CommandLine& cmd)
{
    using namespace arki::utils::commandline;

    dispatchOpts = cmd.createGroup("Options controlling dispatching data to datasets");
    dispatch = dispatchOpts->add< VectorOption<String> >("dispatch", 0, "dispatch", "conffile",
            "dispatch the data to the datasets described in the "
            "given configuration file (or a dataset path can also "
            "be given), then output the metadata of the data that "
            "has been dispatched (can be specified multiple times)");
    testdispatch = dispatchOpts->add< VectorOption<String> >("testdispatch", 0, "testdispatch", "conffile",
            "simulate dispatching the files right after scanning, using the given configuration file "
            "or dataset directory (can be specified multiple times)");
    validate = dispatchOpts->add<StringOption>("validate", 0, "validate", "checks",
            "run the given checks on the input data before dispatching"
            " (comma-separated list; use 'list' to get a list)");
    ignore_duplicates = dispatchOpts->add<BoolOption>("ignore-duplicates", 0, "ignore-duplicates", "",
            "do not consider the run unsuccessful in case of duplicates");

    moveok = dispatchOpts->add<StringOption>("moveok", 0, "moveok", "directory",
            "move input files imported successfully to the given directory");
    moveko = dispatchOpts->add<StringOption>("moveko", 0, "moveko", "directory",
            "move input files with problems to the given directory");
    movework = dispatchOpts->add<StringOption>("movework", 0, "movework", "directory",
            "move input files here before opening them. This is useful to "
            "catch the cases where arki-scan crashes without having a "
            "chance to handle errors.");
    copyok = dispatchOpts->add<StringOption>("copyok", 0, "copyok", "directory",
            "copy the data from input files that was imported successfully to the given directory");
    copyko = dispatchOpts->add<StringOption>("copyko", 0, "copyko", "directory",
            "copy the data from input files that had problems to the given directory");
    status = dispatchOpts->add<BoolOption>("status", 0, "status", "",
            "print to standard error a line per every file with a summary of how it was handled");
}

void ScanOptions::handle_immediate_commands()
{
    // Honour --validate=list
    if (validate->stringValue() == "list")
    {
        // Print validator list and exit
        const ValidatorRepository& vals = ValidatorRepository::get();
        for (ValidatorRepository::const_iterator i = vals.begin();
                i != vals.end(); ++i)
        {
            cout << i->second->name << " - " << i->second->desc << endl;
        }
        throw runtime::HandledByCommandLineParser();
    }
}

std::unique_ptr<MetadataDispatch> ScanOptions::make_dispatcher(runtime::DatasetProcessor& processor)
{
    unique_ptr<MetadataDispatch> dispatcher;

    if (dispatch->isSet() && testdispatch->isSet())
        throw commandline::BadOption("you cannot use --dispatch together with --testdispatch");

    if (!dispatch->isSet() && !testdispatch->isSet())
    {
        if (validate->isSet()) throw commandline::BadOption("--validate only makes sense with --dispatch or --testdispatch");
        if (ignore_duplicates->isSet()) throw commandline::BadOption("--ignore_duplicates only makes sense with --dispatch or --testdispatch");
        if (moveok->isSet()) throw commandline::BadOption("--moveok only makes sense with --dispatch or --testdispatch");
        if (moveko->isSet()) throw commandline::BadOption("--moveko only makes sense with --dispatch or --testdispatch");
        if (movework->isSet()) throw commandline::BadOption("--movework only makes sense with --dispatch or --testdispatch");
        if (copyok->isSet()) throw commandline::BadOption("--copyok only makes sense with --dispatch or --testdispatch");
        if (copyko->isSet()) throw commandline::BadOption("--copyko only makes sense with --dispatch or --testdispatch");
        return dispatcher;
    }

    runtime::readMatcherAliasDatabase();

    if (testdispatch->isSet()) {
        runtime::parseConfigFiles(dispatchInfo, *testdispatch);
        dispatcher.reset(new MetadataDispatch(dispatchInfo, processor, true));
    } else {
        runtime::parseConfigFiles(dispatchInfo, *dispatch);
        dispatcher.reset(new MetadataDispatch(dispatchInfo, processor));
    }

    dispatcher->reportStatus = status->boolValue();
    if (ignore_duplicates->boolValue())
        dispatcher->ignore_duplicates = true;

    if (validate)
    {
        const ValidatorRepository& vals = ValidatorRepository::get();

        // Add validators to dispatcher
        str::Split splitter(validate->stringValue(), ",");
        for (str::Split::const_iterator iname = splitter.begin();
                iname != splitter.end(); ++iname)
        {
            ValidatorRepository::const_iterator i = vals.find(*iname);
            if (i == vals.end())
                throw commandline::BadOption("unknown validator '" + *iname + "'. You can get a list using --validate=list.");
            dispatcher->dispatcher->add_validator(*(i->second));
        }
    }

    if (copyok->isSet())
        dispatcher->dir_copyok = copyok->stringValue();
    if (copyko->isSet())
        dispatcher->dir_copyko = copyko->stringValue();

    return dispatcher;
}


MetadataDispatch::MetadataDispatch(const ConfigFile& cfg, runtime::DatasetProcessor& next, bool test)
    : cfg(cfg), next(next)
{
    timerclear(&startTime);

    if (test)
        dispatcher = new TestDispatcher(cfg, cerr);
    else
        dispatcher = new RealDispatcher(cfg);
}

MetadataDispatch::~MetadataDispatch()
{
    if (dispatcher)
        delete dispatcher;
}

bool MetadataDispatch::process(dataset::Reader& ds, const std::string& name)
{
    setStartTime();
    results.clear();

    if (!dir_copyok.empty())
        copyok.reset(new arki::File(str::joinpath(dir_copyok, str::basename(name)), O_WRONLY | O_APPEND | O_CREAT));
    else
        copyok.release();

    if (!dir_copyko.empty())
        copyko.reset(new arki::File(str::joinpath(dir_copyko, str::basename(name)), O_WRONLY | O_APPEND | O_CREAT));
    else
        copyko.release();

    try {
        ds.query_data(Matcher(), [&](unique_ptr<Metadata> md) { return this->dispatch(move(md)); });
    } catch (std::exception& e) {
        // FIXME: this is a quick experiment: a better message can
        // print some of the stats to document partial imports
        //cerr << i->second->value("path") << ": import FAILED: " << e.what() << endl;
        nag::warning("import FAILED: %s", e.what());
        // Still process what we've got so far
        next.process(results, name);
        throw;
    }

    // Process the resulting annotated metadata as a dataset
    next.process(results, name);

    if (reportStatus)
    {
        cerr << name << ": " << summarySoFar() << endl;
        cerr.flush();
    }

    bool success = !(countNotImported || countInErrorDataset);
    if (ignore_duplicates)
        success = success && (countSuccessful || countDuplicates);
    else
        success = success && (countSuccessful && !countDuplicates);

    flush();

    countSuccessful = 0;
    countNotImported = 0;
    countDuplicates = 0;
    countInErrorDataset = 0;

    return success;
}

bool MetadataDispatch::dispatch(unique_ptr<Metadata>&& md)
{
    // Dispatch to matching dataset
    switch (dispatcher->dispatch(*md))
    {
        case Dispatcher::DISP_OK:
            do_copyok(*md);
            ++countSuccessful;
            break;
        case Dispatcher::DISP_DUPLICATE_ERROR:
            do_copyko(*md);
            ++countDuplicates;
            break;
        case Dispatcher::DISP_ERROR:
            do_copyko(*md);
            ++countInErrorDataset;
            break;
        case Dispatcher::DISP_NOTWRITTEN:
            do_copyko(*md);
            // If dispatching failed, add a big note about it.
            md->add_note("WARNING: The data has not been imported in ANY dataset");
            ++countNotImported;
            break;
    }
    results.acquire(move(md));
    return dispatcher->canContinue();
}

void MetadataDispatch::do_copyok(Metadata& md)
{
    if (copyok && copyok->is_open())
        copyok->write_all_or_throw(md.getData());
}

void MetadataDispatch::do_copyko(Metadata& md)
{
    if (copyko && copyko->is_open())
        copyko->write_all_or_throw(md.getData());
}

void MetadataDispatch::flush()
{
    if (dispatcher) dispatcher->flush();
}

string MetadataDispatch::summarySoFar() const
{
    string timeinfo;
    if (timerisset(&startTime))
    {
        struct timeval now;
        struct timeval diff;
        gettimeofday(&now, NULL);
        timersub(&now, &startTime, &diff);
        char buf[32];
        snprintf(buf, 32, " in %d.%06d seconds", (int)diff.tv_sec, (int)diff.tv_usec);
        timeinfo = buf;
    }
    if (!countSuccessful && !countNotImported && !countDuplicates && !countInErrorDataset)
        return "no data processed" + timeinfo;

    if (!countNotImported && !countDuplicates && !countInErrorDataset)
    {
        stringstream ss;
        ss << "everything ok: " << countSuccessful << " message";
        if (countSuccessful != 1)
            ss << "s";
        ss << " imported" + timeinfo;
        return ss.str();
    }

    stringstream res;

    if (countNotImported)
        res << "serious problems: ";
    else
        res << "some problems: ";

    res << countSuccessful << " ok, "
        << countDuplicates << " duplicates, "
        << countInErrorDataset << " in error dataset";

    if (countNotImported)
        res << ", " << countNotImported << " NOT imported";

    res << timeinfo;

    return res.str();
}

void MetadataDispatch::setStartTime()
{
    gettimeofday(&startTime, NULL);
}




struct ArkiScan : public runtime::ArkiTool
{
    ScanOptions scan;
    MetadataDispatch* dispatcher = nullptr;

    ~ArkiScan()
    {
        delete dispatcher;
    }

    runtime::CommandLine* make_cmdline_parser() override
    {
        unique_ptr<runtime::CommandLine> parser(new runtime::CommandLine("arki-scan", 1));
        parser->usage = "[options] [input...]";
        parser->description =
            "Read one or more files or datasets and process their data "
            "or import them in a dataset.";

        scan.add_to(*parser);

        parser->add(scan.dispatchOpts);

        return parser.release();
    }

    void parse_args(int argc, const char* argv[]) override
    {
        ArkiTool::parse_args(argc, argv);
        scan.handle_immediate_commands();
    }

    void setup_processing() override
    {
        ArkiTool::setup_processing();
        auto d = scan.make_dispatcher(*processor);
        if (d) dispatcher = d.release();
    }

    std::unique_ptr<dataset::Reader> open_source(ConfigFile& info) override
    {
        if (scan.movework->isSet() && info.value("type") == "file")
            info.setValue("path", moveFile(info.value("path"), scan.movework->stringValue()));
        return ArkiTool::open_source(info);
    }

    bool process_source(dataset::Reader& ds, const std::string& name) override
    {
        if (dispatcher)
            return dispatcher->process(ds, name);
        else
            return ArkiTool::process_source(ds, name);
    }

    void close_source(std::unique_ptr<dataset::Reader> ds, bool successful=true) override
    {
        if (successful && scan.moveok->isSet())
        {
            moveFile(*ds, scan.moveok->stringValue());
        }
        else if (!successful && scan.moveko->isSet())
        {
            moveFile(*ds, scan.moveko->stringValue());
        }
        return ArkiTool::close_source(move(ds), successful);
    }
};

}

int main(int argc, const char* argv[])
{
    ArkiScan tool;
    tool.init();
    try {
        tool.parse_args(argc, argv);
        tool.setup_input_info();
        tool.setup_processing();

        bool all_successful = true;
        for (ConfigFile::const_section_iterator i = tool.input_info.sectionBegin();
                i != tool.input_info.sectionEnd(); ++i)
        {
            unique_ptr<dataset::Reader> ds = tool.open_source(*i->second);

            bool success = true;
            try {
                success = tool.process_source(*ds, i->second->value("path"));
            } catch (std::exception& e) {
                // FIXME: this is a quick experiment: a better message can
                // print some of the stats to document partial imports
                cerr << i->second->value("path") << " failed: " << e.what() << endl;
                success = false;
            }

            tool.close_source(move(ds), success);

            // Take note if something went wrong
            if (!success) all_successful = false;
        }

        tool.doneProcessing();

		if (all_successful)
			return 0;
		else
			return 2;
    } catch (runtime::HandledByCommandLineParser& e) {
        return e.status;
    } catch (commandline::BadOption& e) {
        cerr << e.what() << endl;
        tool.args->outputHelp(cerr);
        return 1;
    } catch (std::exception& e) {
        cerr << e.what() << endl;
        return 1;
    }
}
