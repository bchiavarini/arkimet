#include "config.h"
#include "arki/utils/commandline/parser.h"
#include "arki/configfile.h"
#include "arki/dataset.h"
#include "arki/dataset/merged.h"
#include "arki/dataset/http.h"
#include "arki/utils.h"
#include "arki/utils/files.h"
#include "arki/nag.h"
#include "arki/runtime.h"
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
};

struct ArkiQuery : public runtime::ArkiTool
{
    ArkiQueryCommandLine args;

    ArkiQueryCommandLine* get_cmdline_parser() override
    {
        return &args;
    }

    Matcher parse_query(ConfigFile& inputInfo)
    {
        if (args.qmacro->isSet())
            return Matcher::parse("");

        // Resolve the query on each server (including the local system, if
        // queried). If at least one server can expand it, send that
        // expanded query to all servers. If two servers expand the same
        // query in different ways, raise an error.
        set<string> servers_seen;
        string expanded;
        string resolved_by;
        bool first = true;
        for (ConfigFile::const_section_iterator i = inputInfo.sectionBegin();
                i != inputInfo.sectionEnd(); ++i)
        {
            string server = i->second->value("server");
            if (servers_seen.find(server) != servers_seen.end()) continue;
            string got;
            try {
                if (server.empty())
                {
                    got = Matcher::parse(args.strquery).toStringExpanded();
                    resolved_by = "local system";
                } else {
                    got = dataset::http::Reader::expandMatcher(args.strquery, server);
                    resolved_by = server;
                }
            } catch (std::exception& e) {
                // If the server cannot expand the query, we're
                // ok as we send it expanded. What we are
                // checking here is that the server does not
                // have a different idea of the same aliases
                // that we use
                continue;
            }
            if (!first && got != expanded)
            {
                nag::warning("%s expands the query as %s", server.c_str(), got.c_str());
                nag::warning("%s expands the query as %s", resolved_by.c_str(), expanded.c_str());
                throw std::runtime_error("cannot check alias consistency: two systems queried disagree about the query alias expansion");
            } else if (first)
                expanded = got;
            first = false;
        }

        // If no server could expand it, do it anyway locally: either we
        // can resolve it with local aliases, or we raise an appropriate
        // error message
        if (first)
            expanded = args.strquery;

        return Matcher::parse(expanded);
    }


    void configure() override
    {
        ArkiTool::configure();

        // Filter the dataset list
        if (args.restr->isSet())
        {
            runtime::Restrict rest(args.restr->stringValue());
            rest.remove_unallowed(input_info);
            if (input_info.sectionSize() == 0)
                throw commandline::BadOption("no accessible datasets found for the given --restrict value");
        }
    }

    Matcher make_query() override
    {
        return parse_query(input_info);
    }

    int main() override
    {
        bool all_successful = true;
        if (args.merged->boolValue())
        {
            dataset::Merged merger;
            size_t dscount = input_info.sectionSize();
            std::vector<unique_ptr<dataset::Reader>> datasets(dscount);

            // Instantiate the datasets and add them to the merger
            int idx = 0;
            string names;
            for (ConfigFile::const_section_iterator i = input_info.sectionBegin();
                    i != input_info.sectionEnd(); ++i, ++idx)
            {
                datasets[idx] = open_source(*i->second);
                merger.addDataset(*datasets[idx]);
                if (names.empty())
                    names = i->first;
                else
                    names += ","+i->first;
            }

            // Perform the query
            all_successful = process_source(merger, names);

            for (size_t i = 0; i < dscount; ++i)
                close_source(move(datasets[i]), all_successful);
        } else if (args.qmacro->isSet()) {
            // Create the virtual qmacro dataset
            ConfigFile cfg;
            unique_ptr<dataset::Reader> ds = runtime::make_qmacro_dataset(
                    cfg,
                    input_info,
                    args.qmacro->stringValue(),
                    args.strquery);

            // Perform the query
            all_successful = process_source(*ds, args.qmacro->stringValue());
        } else {
            // Query all the datasets in sequence
            for (ConfigFile::const_section_iterator i = input_info.sectionBegin();
                    i != input_info.sectionEnd(); ++i)
            {
                unique_ptr<dataset::Reader> ds = open_source(*i->second);
                nag::verbose("Processing %s...", i->second->value("path").c_str());
                bool success = process_source(*ds, i->second->value("path"));
                close_source(move(ds), success);
                if (!success) all_successful = false;
            }
        }

        doneProcessing();

        if (all_successful)
            return 0;
        else
            return 2;
        //return summary.count() > 0 ? 0 : 1;
    }
};

}

int main(int argc, const char* argv[])
{
    ArkiQuery tool;
    return tool.run(argc, argv);
}
