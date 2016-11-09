#include "arkiquery.h"
#include "arki/dataset/http.h"
#include "arki/dataset/merged.h"
#include "arki/nag.h"

using namespace std;

namespace arki {
namespace runtime {

void ArkiQuery::print_config(FILE* out) const
{
    ArkiTool::print_config(out);
    fprintf(out, "qmacro: %s\n", qmacro.c_str());;
    fprintf(out, "merged: %s\n", merged ? "true" : "false");
}

void ArkiQuery::set_query(const std::string& strquery)
{
    if (!qmacro.empty())
        return ArkiTool::set_query("");

    // Resolve the query on each server (including the local system, if
    // queried). If at least one server can expand it, send that
    // expanded query to all servers. If two servers expand the same
    // query in different ways, raise an error.
    set<string> servers_seen;
    string expanded;
    string resolved_by;
    bool first = true;
    for (ConfigFile::const_section_iterator i = input_info.sectionBegin();
            i != input_info.sectionEnd(); ++i)
    {
        string server = i->second->value("server");
        if (servers_seen.find(server) != servers_seen.end()) continue;
        string got;
        try {
            if (server.empty())
            {
                got = Matcher::parse(strquery).toStringExpanded();
                resolved_by = "local system";
            } else {
                got = dataset::http::Reader::expandMatcher(strquery, server);
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
        expanded = strquery;

    return ArkiTool::set_query(expanded);
}

int ArkiQuery::main()
{
    ArkiTool::main();

    bool all_successful = true;
    if (merged)
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
    } else if (!qmacro.empty()) {
        // Create the virtual qmacro dataset
        ConfigFile cfg;
        unique_ptr<dataset::Reader> ds = runtime::make_qmacro_dataset(
                cfg,
                input_info,
                qmacro,
                strquery);

        // Perform the query
        all_successful = process_source(*ds, qmacro);
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
}

}
}
