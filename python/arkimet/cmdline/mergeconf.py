import arkimet as arki
from arkimet.cmdline.base import App, Fail
import re
import sys
import logging

log = logging.getLogger("arki-mergeconf")

re_stringlist = re.compile(r"[\s,]+")


class Mergeconf(App):
    """
    Read dataset configuration from the given directories or config files,
    merge them and output the merged config file to standard output
    """

    def __init__(self):
        super().__init__()
        self.parser.add_argument("-o", "--output", metavar="file",
                                 help="write the output to the given file instead of standard output")
        self.parser.add_argument("--extra", action="store_true",
                                 help="extract extra information from the datasets (such as bounding box)"
                                      " and include it in the configuration")
        self.parser.add_argument("--ignore-system-datasets", action="store_true",
                                 help="ignore error and duplicates datasets")
        self.parser.add_argument("--restrict", metavar="names", action="store",
                                 help="restrict operations to only those datasets that allow one of the given"
                                      " (comma separated) names")
        self.parser.add_argument("-C", "--config", metavar="file", action="append",
                                 help="merge configuration from the given file (can be given more than once)")
        self.parser.add_argument("sources", nargs="*", action="store",
                                 help="datasets, configuration files or remote urls")

    def _add_section(self, merged, section, name=None):
        if name is None:
            name = section["name"]

        old = merged.section(name)
        if old is not None:
            log.warning("ignoring dataset %s in %s, which has the same name as the dataset in %s",
                        name, section["path"], old["path"])
        merged[name] = section
        merged[name]["name"] = name

    def run(self):
        super().run()

        merged = arki.cfg.Sections()

        # Process --config options
        if self.args.config is not None:
            for pathname in self.args.config:
                if pathname == "-":
                    cfg = arki.cfg.Sections.parse(sys.stdin)
                else:
                    cfg = arki.cfg.Sections.parse(pathname)

                for name, section in cfg.items():
                    self._add_section(merged, section, name)

        # Read the config files from the remaining commandline arguments
        for path in self.args.sources:
            if path.startswith("http://") or path.startswith("https://"):
                sections = arki.dataset.http.load_cfg_sections(path)
                for name, section in sections.items():
                    self._add_section(merged, section, name)
            else:
                section = arki.dataset.read_config(path)
                self._add_section(merged, section)

        if not merged:
            raise Fail("you need to specify at least one config file or dataset")

        # Remove unallowed entries
        if self.args.restrict:
            to_remove = []
            allowed = frozenset(x for x in re_stringlist.split(self.args.restrict) if x)
            for name, section in merged.items():
                r = section.get("restrict", None)
                if r is None:
                    to_remove.append(name)
                    continue
                offered = frozenset(x for x in re_stringlist.split(r) if x)
                if not (allowed & offered):
                    to_remove.append(name)

            for name in to_remove:
                del merged[name]

        if self.args.ignore_system_datasets:
            to_remove = []

            for name, section in merged.items():
                type = section.get("type")
                name = section.get("name")

                if (type == "error" or type == "duplicates" or
                        (type == "remote" and (name == "error" or name == "duplicates"))):
                    to_remove.append(name)

            for name in to_remove:
                del merged[name]

        if not merged:
            raise Fail("none of the configuration provided were useable")

        # Validate the configuration
        has_errors = False
        for name, section in merged.items():
            # Validate filters
            filter = section.get("filter")
            if filter is None:
                continue

            try:
                arki.Matcher(filter)
            except ValueError as e:
                print("{}: {}".format(name, e), file=sys.stderr)
                has_errors = True
        if has_errors:
            raise Fail("Some input files did not validate.")

        # If requested, compute extra information
        if self.args.extra:
            for name, section in merged.items():
                # Instantiate the dataset
                ds = arki.dataset.Reader(section)
                # Get the summary
                summary = ds.query_summary()
                # Compute bounding box, and store the WKT in bounding
                bbox = summary.get_convex_hull()
                if bbox:
                    section["bounding"] = bbox

        # Output the merged configuration
        if self.args.output:
            with open(self.args.output, "wt") as fd:
                merged.write(fd)
        else:
            merged.write(sys.stdout)


if __name__ == "__main__":
    sys.exit(Mergeconf.main())
