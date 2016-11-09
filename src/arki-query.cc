#include "config.h"
#include "arki/utils/commandline/parser.h"
#include "arki/configfile.h"
#include "arki/dataset.h"
#include "arki/utils.h"
#include "arki/utils/files.h"
#include "arki/runtime/arkiquery.h"
#include <cstring>
#include <iostream>

using namespace std;
using namespace arki;
using namespace arki::utils;

namespace {
struct ArkiQueryCommandLine : public runtime::CommandLine
{
    utils::commandline::StringOption* exprfile = nullptr;
    utils::commandline::BoolOption* merged = nullptr;
    utils::commandline::StringOption* qmacro = nullptr;
    utils::commandline::StringOption* restr = nullptr;
    std::string strquery;

    ArkiQueryCommandLine() : runtime::CommandLine("arki-query", 1)
    {
        using namespace arki::utils::commandline;

        usage = "[options] [expression] [configfile or directory...]";
        description =
            "Query the datasets in the given config file for data matching the"
            " given expression, and output the matching metadata.";

        exprfile = inputOpts->add<StringOption>("file", 'f', "file", "file",
            "read the expression from the given file");
        merged = outputOpts->add<BoolOption>("merged", 0, "merged", "",
            "if multiple datasets are given, merge their data and output it in"
            " reference time order.  Note: sorting does not work when using"
            " --postprocess, --data or --report");
        qmacro = add<StringOption>("qmacro", 0, "qmacro", "name",
            "run the given query macro instead of a plain query");
        restr = add<StringOption>("restrict", 0, "restrict", "names",
                "restrict operations to only those datasets that allow one of the given (comma separated) names");
    }

    void parse_positional_args() override
    {
        // Parse the matcher query
        if (exprfile->isSet())
        {
            // Read the entire file into memory and parse it as an expression
            strquery = files::read_file(exprfile->stringValue());
        } else {
            // Read from the first commandline argument
            if (!hasNext())
            {
                if (exprfile)
                    throw commandline::BadOption("you need to specify a filter expression or use --" + exprfile->longNames[0]);
                else
                    throw commandline::BadOption("you need to specify a filter expression");
            }
            // And parse it as an expression
            strquery = next();
        }

        CommandLine::parse_positional_args();
    }

    ConfigFile get_inputs() override
    {
        ConfigFile cfg = CommandLine::get_inputs();

        // Filter the dataset list
        if (restr->isSet())
        {
            runtime::Restrict rest(restr->stringValue());
            rest.remove_unallowed(cfg);
            if (cfg.sectionSize() == 0)
                throw commandline::BadOption("no accessible datasets found for the given --restrict value");
        }

        return cfg;
    }

    void configure(runtime::ArkiQuery& tool)
    {
        CommandLine::configure(tool);
        tool.qmacro = qmacro->stringValue();
        tool.merged = merged->boolValue();
        tool.set_query(strquery);
    }
};

}

int main(int argc, const char* argv[])
{
    runtime::init();
    ArkiQueryCommandLine args;
    try {
        args.parse_all(argc, argv);
        runtime::ArkiQuery tool;
        args.configure(tool);
        return tool.main();
    } catch (runtime::HandledByCommandLineParser& e) {
        return e.status;
    } catch (commandline::BadOption& e) {
        cerr << e.what() << endl;
        args.outputHelp(cerr);
        return 1;
    } catch (std::exception& e) {
        cerr << e.what() << endl;
        return 1;
    }
}
