#include "config.h"
#include <arki/utils/commandline/parser.h>
#include <arki/configfile.h>
#include <arki/dataset.h>
#include <arki/dataset/merged.h>
#include <arki/dataset/http.h>
#include <arki/utils.h>
#include <arki/nag.h>
#include <arki/runtime.h>
#include <cstring>
#include <iostream>

using namespace std;
using namespace arki;
using namespace arki::utils;

namespace arki {
namespace utils {
namespace commandline {

struct Options : public arki::runtime::CommandLine
{
	Options() : runtime::CommandLine("arki-query", 1)
	{
		usage = "[options] [expression] [configfile or directory...]";
		description =
		    "Query the datasets in the given config file for data matching the"
			" given expression, and output the matching metadata.";

		addQueryOptions();
	}
};

}
}
}

int main(int argc, const char* argv[])
{
    commandline::Options opts;
    try {
        if (opts.parse(argc, argv))
            return 0;

		runtime::init();

		opts.setupProcessing();

        bool all_successful = true;
        if (opts.merged->boolValue())
        {
            dataset::Merged merger;
            size_t dscount = opts.inputInfo.sectionSize();
            std::vector<unique_ptr<dataset::Reader>> datasets(dscount);

			// Instantiate the datasets and add them to the merger
			int idx = 0;
			string names;
			for (ConfigFile::const_section_iterator i = opts.inputInfo.sectionBegin();
					i != opts.inputInfo.sectionEnd(); ++i, ++idx)
			{
				datasets[idx] = opts.openSource(*i->second);
				merger.addDataset(*datasets[idx]);
				if (names.empty())
					names = i->first;
				else
					names += ","+i->first;
			}

			// Perform the query
			all_successful = opts.processSource(merger, names);

            for (size_t i = 0; i < dscount; ++i)
                opts.closeSource(move(datasets[i]), all_successful);
        } else if (opts.qmacro->isSet()) {
            // Create the virtual qmacro dataset
            ConfigFile cfg;
            unique_ptr<dataset::Reader> ds = runtime::make_qmacro_dataset(
                    cfg,
                    opts.inputInfo,
                    opts.qmacro->stringValue(),
                    opts.strquery);

            // Perform the query
            all_successful = opts.processSource(*ds, opts.qmacro->stringValue());
        } else {
            // Query all the datasets in sequence
            for (ConfigFile::const_section_iterator i = opts.inputInfo.sectionBegin();
                    i != opts.inputInfo.sectionEnd(); ++i)
            {
                unique_ptr<dataset::Reader> ds = opts.openSource(*i->second);
                nag::verbose("Processing %s...", i->second->value("path").c_str());
                bool success = opts.processSource(*ds, i->second->value("path"));
                opts.closeSource(move(ds), success);
                if (!success) all_successful = false;
            }
        }

		opts.doneProcessing();

		if (all_successful)
			return 0;
		else
			return 2;
		//return summary.count() > 0 ? 0 : 1;
    } catch (commandline::BadOption& e) {
        cerr << e.what() << endl;
        opts.outputHelp(cerr);
        return 1;
	} catch (std::exception& e) {
		cerr << e.what() << endl;
		return 1;
	}
}
