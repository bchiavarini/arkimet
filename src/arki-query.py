#!/usr/bin/python3

import argparse
import sys

def main():
    parser = argparse.ArgumentParser(
        description="Query the datasets in the given config file for data matching the"
                    " given expression, and output the matching metadata.")

    parser_info = parser.add_argument_group("Options controlling verbosity")
    parser_info.add_argument("--debug", action="store_true", help="debug output")
    parser_info.add_argument("--verbose", action="store_true", help="verbose output")
    parser_info.add_argument("--file", metavar="file", action="store", help="read the expression from the given file")

    parser_input = parser.add_argument_group("Options controlling input data")
    parser_input.add_argument("--files", metavar="file", action="store", help="read the list of files to scan from the given file instead of the command line")
    parser_input.add_argument("--config", "-C", metavar="file", action="append", help="read configuration about input sources from the given file (can be given more than once)")
    parser_input.add_argument("--postproc-data", metavar="file", action="append", help=
            "when querying a remote server with postprocessing, upload a file"
            " to be used by the postprocessor (can be given more than once)")

    parser_out = parser.add_argument_group("Options controlling output style")
    parser_out.add_argument("--yaml", "--dump", action="store_true", help="dump the metadata as human-readable Yaml records")
    parser_out.add_argument("--json", action="store_true", help="dump the metadata in JSON format")
    parser_out.add_argument("--annotate", action="store_true", help="annotate the human-readable Yaml output with field descriptions")
    parser_out.add_argument("--inline", action="store_true", help="output the binary metadata together with the data (pipe through "
                                                                  " arki-dump or arki-grep to estract the data afterwards)")
    parser_out.add_argument("--data", action="store_true", help="output only the data")
    parser_out.add_argument("--postprocess", "-p", metavar="command", action="store", default="", help="output only the data, postprocessed with the given filter")
    parser_out.add_argument("--report", metavar="name", action="store", default="", help="produce the given report with the command output")
    parser_out.add_argument("--output", metavar="file", action="store", default="-", help="write the output to the given file instead of standard output")
    parser_out.add_argument("--targetfile", metavar="pattern", action="store", default="", help="append the output to file names computed from the data")
    parser_out.add_argument("--summary", action="store_true", help="output only the summary of the data")
    parser_out.add_argument("--summary-short", action="store_true", help="output a list of all metadata values that exist in the summary of the data")
    parser_out.add_argument("--summary-restrict", metavar="types", action="store", default="", help="summarise using only the given metadata types (comma-separated list)")
    parser_out.add_argument("--sort", metavar="period:order", action="store", default="", help=
            "sort order.  Period can be year, month, day, hour or minute."
            " Order can be a list of one or more metadata"
            " names (as seen in --yaml field names), with a '-'"
            " prefix to indicate reverse order.  For example,"
            " 'day:origin, -timerange, reftime' orders one day at a time"
            " by origin first, then by reverse timerange, then by reftime."
            " Default: do not sort")
    parser_out.add_argument("--merged", action="store_true", help=
            "if multiple datasets are given, merge their data and output it in"
            " reference time order.  Note: sorting does not work when using"
            " --postprocess, --data or --report")

    parser.add_argument("--qmacro", metavar="name", action="store", default="", help="run the given query macro instead of a plain query")
    parser.add_argument("--restrict", metavar="names", action="store", default="", help="restrict operations to only those datasets that allow one of the given (comma separated) names")

    parser.add_argument("query", help="expression used to select data (not used if using --file)")
    parser.add_argument("input", nargs="*", help="input configuration files or directories")

    args = parser.parse_args()

    import arkimet as arki

    inputs = []
    if args.file:
        inputs.append(args.query)
        with open(args.file, "rt") as fd:
            query = fd.read().strip()
    else:
        query = args.query
    inputs.extend(args.input)

    all_input = {}

    # TODO: python bindings for parseConfigFiles() returning dict() (or can be implemented with configparser maybe?)
    if args.config:
        for f in args.config:
            pass # parseConfigFile(f, *cfgfiles);

    if args.files:
        if args.files == "-":
            files = list(sys.stdin)
        else:
            with open(args.files, "rt") as fd:
                files = list(fd)

        for f in files:
            if not f: continue
            for name, sec in arki.read_config(f).items():
                all_input[name] = sec

    for i in inputs:
        for name, sec in arki.read_config(i).items():
            all_input[name] = sec

    if not all_input:
        raise RuntimeError("you need to specify at least one input file or dataset")

    arki.arki_query(
        query=query,
        input=all_input,
        summary=args.summary,
        summary_short=args.summary_short,
        yaml=args.yaml,
        json=args.json,
        annotate=args.annotate,
        data_only=args.data,
        data_inline=args.inline,
        postprocess=args.postprocess,
        report=args.report,
        summary_restrict=args.summary_restrict,
        sort=args.sort,
        targetfile=args.targetfile,
        output=args.output,
        postproc_data=":".join(args.postproc_data) if args.postproc_data else "",
        qmacro=args.qmacro,
        merged=args.merged,
    )

if __name__ == "__main__":
    main()

