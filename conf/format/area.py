from arkimet.formatter import Formatter
from arkimet.formatter.eccodes import GribTable


def format_area(v):
    if v["style"] == "GRIB":
        values = v.get("val")
        # values is now a dict
        if not values or not values["type"]:
            return None
        drt = GribTable.load(1, "6")
        if not drt:
            return None
        out = drt.abbr(values["type"]) + " - " + drt.desc(values["type"]) + ": "
        # FIXME: something is missing here?
        return out
    elif v["style"] == "VM2":
        desc = "id={}".format(v["id"])
        for k, v in v["dval"].items():
            desc += ", {}={}".format(k, v)
        return desc


Formatter.register("area", format_area)
