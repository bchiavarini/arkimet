import arkimet as arki
from arkimet.cmdline.base import App, Fail
import sys
import logging

log = logging.getLogger("arki-mergeconf")


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

        arki_mergeconf = arki.ArkiMergeconf()
        arki_mergeconf.run(
            merged,
            restrict=self.args.restrict,
            ignore_system_datasets=self.args.ignore_system_datasets,
            extra=self.args.extra,
        )

        # Output the merged configuration
        if self.args.output:
            with open(self.args.output, "wt") as fd:
                merged.write(fd)
        else:
            merged.write(sys.stdout)


if __name__ == "__main__":
    sys.exit(Mergeconf.main())
