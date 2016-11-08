#include "config.h"
#include "arki/runtime.h"
#include "arki/exceptions.h"
#include "arki/utils/string.h"
#include "arki/configfile.h"
#include "arki/summary.h"
#include "arki/matcher.h"
#include "arki/utils.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/dataset.h"
#include "arki/dataset/file.h"
#include "arki/dataset/http.h"
#include "arki/dataset/index/base.h"
#include "arki/targetfile.h"
#include "arki/formatter.h"
#include "arki/postprocess.h"
#include "arki/querymacro.h"
#include "arki/sort.h"
#include "arki/nag.h"
#include "arki/iotrace.h"
#include "arki/types-init.h"
#include "arki/utils/sys.h"
#ifdef HAVE_LUA
#include "arki/report.h"
#endif
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
#include <cstdlib>
#include <cassert>

using namespace std;
using namespace arki::utils;
using namespace arki::types;

namespace arki {
namespace runtime {

void init()
{
    static bool initialized = false;

    if (initialized) return;
    types::init_default_types();
    runtime::readMatcherAliasDatabase();
    iotrace::init();
    initialized = true;
}

HandledByCommandLineParser::HandledByCommandLineParser(int status) : status(status) {}
HandledByCommandLineParser::~HandledByCommandLineParser() {}

std::unique_ptr<dataset::Reader> make_qmacro_dataset(const ConfigFile& ds_cfg, const ConfigFile& dispatch_cfg, const std::string& qmacroname, const std::string& query, const std::string& url)
{
    unique_ptr<dataset::Reader> ds;
    string baseurl = dataset::http::Reader::allSameRemoteServer(dispatch_cfg);
    if (baseurl.empty())
    {
        // Create the local query macro
        nag::verbose("Running query macro %s on local datasets", qmacroname.c_str());
        ds.reset(new Querymacro(ds_cfg, dispatch_cfg, qmacroname, query));
    } else {
        // Create the remote query macro
        nag::verbose("Running query macro %s on %s", qmacroname.c_str(), baseurl.c_str());
        ConfigFile cfg;
        cfg.setValue("name", qmacroname);
        cfg.setValue("type", "remote");
        cfg.setValue("path", baseurl);
        cfg.setValue("qmacro", query);
        if (!url.empty())
            cfg.setValue("url", url);
        ds = dataset::Reader::create(cfg);
    }
    return ds;
}

CommandLine::CommandLine(const std::string& name, int mansection)
    : StandardParserWithManpage(name, PACKAGE_VERSION, mansection, PACKAGE_BUGREPORT)
{
    using namespace arki::utils::commandline;

	infoOpts = createGroup("Options controlling verbosity");
	debug = infoOpts->add<BoolOption>("debug", 0, "debug", "", "debug output");
	verbose = infoOpts->add<BoolOption>("verbose", 0, "verbose", "", "verbose output");

    // Used only if requested
    inputOpts = createGroup("Options controlling input data");
    files = inputOpts->add<StringOption>("files", 0, "files", "file",
            "read the list of files to scan from the given file instead of the command line");
    cfgfiles = inputOpts->add< VectorOption<String> >("config", 'C', "config", "file",
            "read configuration about input sources from the given file (can be given more than once)");

	outputOpts = createGroup("Options controlling output style");
	yaml = outputOpts->add<BoolOption>("yaml", 0, "yaml", "",
			"dump the metadata as human-readable Yaml records");
	yaml->longNames.push_back("dump");
	json = outputOpts->add<BoolOption>("json", 0, "json", "",
			"dump the metadata in JSON format");
	annotate = outputOpts->add<BoolOption>("annotate", 0, "annotate", "",
			"annotate the human-readable Yaml output with field descriptions");
	dataInline = outputOpts->add<BoolOption>("inline", 0, "inline", "",
			"output the binary metadata together with the data (pipe through "
			" arki-dump or arki-grep to estract the data afterwards)");
	dataOnly = outputOpts->add<BoolOption>("data", 0, "data", "",
			"output only the data");
	postprocess = outputOpts->add<StringOption>("postproc", 'p', "postproc", "command",
			"output only the data, postprocessed with the given filter");
#ifdef HAVE_LUA
	report = outputOpts->add<StringOption>("report", 0, "report", "name",
			"produce the given report with the command output");
#endif
	outfile = outputOpts->add<StringOption>("output", 'o', "output", "file",
			"write the output to the given file instead of standard output");
	targetfile = outputOpts->add<StringOption>("targetfile", 0, "targetfile", "pattern",
			"append the output to file names computed from the data"
			" to be written. See /etc/arkimet/targetfile for details.");
	summary = outputOpts->add<BoolOption>("summary", 0, "summary", "",
			"output only the summary of the data");
	summary_short = outputOpts->add<BoolOption>("summary-short", 0, "summary-short", "",
			"output a list of all metadata values that exist in the summary of the data");
	summary_restrict = outputOpts->add<StringOption>("summary-restrict", 0, "summary-restrict", "types",
			"summarise using only the given metadata types (comma-separated list)");
	sort = outputOpts->add<StringOption>("sort", 0, "sort", "period:order",
			"sort order.  Period can be year, month, day, hour or minute."
			" Order can be a list of one or more metadata"
			" names (as seen in --yaml field names), with a '-'"
			" prefix to indicate reverse order.  For example,"
			" 'day:origin, -timerange, reftime' orders one day at a time"
			" by origin first, then by reverse timerange, then by reftime."
			" Default: do not sort");

	postproc_data = inputOpts->add< VectorOption<ExistingFile> >("postproc-data", 0, "postproc-data", "file",
		"when querying a remote server with postprocessing, upload a file"
		" to be used by the postprocessor (can be given more than once)");
}

CommandLine::~CommandLine()
{
}

bool CommandLine::parse(int argc, const char* argv[])
{
    add(infoOpts);
    add(inputOpts);
    add(outputOpts);

	if (StandardParserWithManpage::parse(argc, argv))
		return true;

	nag::init(verbose->isSet(), debug->isSet());

    if (postprocess->isSet() && targetfile->isSet())
        throw commandline::BadOption("--postprocess conflicts with --targetfile");
    if (postproc_data && postproc_data->isSet() && !postprocess->isSet())
        throw commandline::BadOption("--postproc-data only makes sense with --postprocess");
    if (summary->isSet() && summary_short->isSet())
        throw commandline::BadOption("--summary and --summary-short cannot be used together");

    return false;
}

void CommandLine::parse_positional_args()
{
    while (hasNext()) // From command line arguments, looking for data files or datasets
        input_args.push_back(next());
}

void CommandLine::parse_all(int argc, const char* argv[])
{
    if (parse(argc, argv))
        throw HandledByCommandLineParser(0);
    parse_positional_args();
}

ConfigFile CommandLine::get_inputs()
{
    ConfigFile input_info;

    // Initialise the dataset list
    parseConfigFiles(input_info, *cfgfiles);

    if (files->isSet())    // From --files option, looking for data files or datasets
    {
        // Open the file
        string file = files->stringValue();
        unique_ptr<NamedFileDescriptor> in;
        if (file != "-")
            in.reset(new InputFile(file));
        else
            in.reset(new Stdin);

        // Read the content and scan the related files or dataset directories
        auto reader = LineReader::from_fd(*in);
        string line;
        while (reader->getline(line))
        {
            line = str::strip(line);
            if (line.empty())
                continue;
            dataset::Reader::readConfig(line, input_info);
        }
    }

    // From command line arguments, looking for data files or datasets
    for (const auto& i: input_args)
        dataset::Reader::readConfig(i, input_info);

    if (input_info.sectionSize() == 0)
        throw commandline::BadOption("you need to specify at least one input file or dataset");

    return input_info;
}

ArkiTool::~ArkiTool()
{
    delete processor;
    delete output;
}

void ArkiTool::configure()
{
    CommandLine* args = get_cmdline_parser();

    // Initialize the processor maker
    pmaker.summary = args->summary->boolValue();
    pmaker.summary_short = args->summary_short->boolValue();
    pmaker.yaml = args->yaml->boolValue();
    pmaker.json = args->json->boolValue();
    pmaker.annotate = args->annotate->boolValue();
    pmaker.data_only = args->dataOnly->boolValue();
    pmaker.data_inline = args->dataInline->boolValue();
    pmaker.postprocess = args->postprocess->stringValue();
    pmaker.report = args->report->stringValue();
    pmaker.summary_restrict = args->summary_restrict->stringValue();
    pmaker.sort = args->sort->stringValue();
    pmaker.targetfile = args->targetfile->stringValue();

    // Run here a consistency check on the processor maker configuration
    std::string errors = pmaker.verify_option_consistency();
    if (!errors.empty())
        throw commandline::BadOption(errors);

    input_info = args->get_inputs();

    // Open output stream
    if (!output)
        output = make_output(*args->outfile).release();

    if (args->postproc_data->isSet())
    {
        // Pass files for the postprocessor in the environment
        string val = str::join(":", args->postproc_data->values().begin(), args->postproc_data->values().end());
        setenv("ARKI_POSTPROC_FILES", val.c_str(), 1);
    } else
        unsetenv("ARKI_POSTPROC_FILES");

    // Create the core processor
    Matcher query = make_query();
    unique_ptr<DatasetProcessor> p = pmaker.make(query, *output);
    processor = p.release();

}

Matcher ArkiTool::make_query()
{
    return Matcher();
}

void ArkiTool::doneProcessing()
{
    if (processor)
        processor->end();
}

std::unique_ptr<dataset::Reader> ArkiTool::open_source(ConfigFile& info)
{
    return unique_ptr<dataset::Reader>(dataset::Reader::create(info));
}

bool ArkiTool::process_source(dataset::Reader& ds, const std::string& name)
{
    processor->process(ds, name);
    return true;
}

void ArkiTool::close_source(std::unique_ptr<dataset::Reader> ds, bool successful)
{
    // TODO: print status
    // ds will be automatically deallocated here
}

int ArkiTool::run(int argc, const char* argv[])
{
    runtime::init();
    try {
        get_cmdline_parser()->parse_all(argc, argv);
        configure();
        return main();
    } catch (runtime::HandledByCommandLineParser& e) {
        return e.status;
    } catch (commandline::BadOption& e) {
        cerr << e.what() << endl;
        get_cmdline_parser()->outputHelp(cerr);
        return 1;
    } catch (std::exception& e) {
        cerr << e.what() << endl;
        return 1;
    }
}

}
}
