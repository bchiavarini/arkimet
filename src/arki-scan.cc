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
#include "arki/runtime/dispatcher.h"
#include "config.h"
#include <memory>
#include <iostream>
#include <cstdio>

using namespace std;
using namespace arki;
using namespace arki::utils;

namespace {

struct ArkiScanCommandLine : public runtime::CommandLine
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

    ArkiScanCommandLine() : runtime::CommandLine("arki-scan", 1)
    {
        using namespace arki::utils::commandline;

        usage = "[options] [input...]";
        description =
            "Read one or more files or datasets and process their data "
            "or import them in a dataset.";

        dispatchOpts = createGroup("Options controlling dispatching data to datasets");
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

        add(dispatchOpts);
    }

    bool parse(int argc, const char* argv[]) override
    {
        if (CommandLine::parse(argc, argv))
            return true;

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
            return true;
        }

        return false;
    }

    std::unique_ptr<runtime::MetadataDispatch> make_dispatcher(runtime::DatasetProcessor& processor)
    {
        ConfigFile dispatchInfo;

        unique_ptr<runtime::MetadataDispatch> dispatcher;

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

        if (testdispatch->isSet()) {
            runtime::parseConfigFiles(dispatchInfo, *testdispatch);
            dispatcher.reset(new runtime::MetadataDispatch(dispatchInfo, processor, true));
        } else {
            runtime::parseConfigFiles(dispatchInfo, *dispatch);
            dispatcher.reset(new runtime::MetadataDispatch(dispatchInfo, processor));
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
};

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




struct ArkiScan : public runtime::ArkiTool
{
    ArkiScanCommandLine args;
    runtime::MetadataDispatch* dispatcher = nullptr;

    ~ArkiScan()
    {
        delete dispatcher;
    }

    ArkiScanCommandLine* get_cmdline_parser() override
    {
        return &args;
    }

    void setup_processing() override
    {
        ArkiTool::setup_processing();
        auto d = args.make_dispatcher(*processor);
        if (d) dispatcher = d.release();
    }

    std::unique_ptr<dataset::Reader> open_source(ConfigFile& info) override
    {
        if (args.movework->isSet() && info.value("type") == "file")
            info.setValue("path", moveFile(info.value("path"), args.movework->stringValue()));
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
        if (successful && args.moveok->isSet())
        {
            moveFile(*ds, args.moveok->stringValue());
        }
        else if (!successful && args.moveko->isSet())
        {
            moveFile(*ds, args.moveko->stringValue());
        }
        return ArkiTool::close_source(move(ds), successful);
    }

    int main() override
    {
        bool all_successful = true;
        for (ConfigFile::const_section_iterator i = input_info.sectionBegin();
                i != input_info.sectionEnd(); ++i)
        {
            unique_ptr<dataset::Reader> ds = open_source(*i->second);

            bool success = true;
            try {
                success = process_source(*ds, i->second->value("path"));
            } catch (std::exception& e) {
                // FIXME: this is a quick experiment: a better message can
                // print some of the stats to document partial imports
                cerr << i->second->value("path") << " failed: " << e.what() << endl;
                success = false;
            }

            close_source(move(ds), success);

            // Take note if something went wrong
            if (!success) all_successful = false;
        }

        doneProcessing();

        if (all_successful)
            return 0;
        else
            return 2;
    }
};

}

int main(int argc, const char* argv[])
{
    ArkiScan tool;
    return tool.run(argc, argv);
}
