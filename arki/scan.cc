#include "scan.h"
#include "arki/libconfig.h"
#include "arki/segment.h"
#include "arki/metadata.h"
#include "arki/core/file.h"
#include "arki/types/source/blob.h"
#include "arki/utils.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/utils/files.h"
#ifdef HAVE_GRIBAPI
#include "arki/scan/grib.h"
#endif
#ifdef HAVE_DBALLE
#include "arki/scan/bufr.h"
#endif
#ifdef HAVE_HDF5
#include "arki/scan/odimh5.h"
#endif
#ifdef HAVE_VM2
#include "arki/scan/vm2.h"
#endif

using namespace std;
using namespace arki::utils;

namespace arki {
namespace scan {

Scanner::~Scanner()
{
}

void Scanner::open(const std::string& filename, std::shared_ptr<segment::Reader> reader)
{
    close();
    this->filename = filename;
    this->reader = reader;
}

void Scanner::close()
{
    filename.clear();
    reader.reset();
}

void Scanner::test_open(const std::string& filename)
{
    string basedir, relpath;
    utils::files::resolve_path(filename, basedir, relpath);
    open(sys::abspath(filename), Segment::detect_reader(format_from_filename(filename), basedir, relpath, filename, make_shared<core::lock::Null>()));
}

bool Scanner::scan_file(const std::string& abspath, std::shared_ptr<segment::Reader> reader, metadata_dest_func dest)
{
    open(abspath, reader);
    while (true)
    {
        unique_ptr<Metadata> md(new Metadata);
        if (!next(*md)) break;
        if (!dest(move(md))) return false;
    }
    return true;
}

bool Scanner::scan_metadata(const std::string& abspath, metadata_dest_func dest)
{
    open(abspath, std::shared_ptr<segment::Reader>());
    while (true)
    {
        unique_ptr<Metadata> md(new Metadata);
        if (!next(*md)) break;
        if (!dest(move(md))) return false;
    }
    return true;
}

std::unique_ptr<Scanner> Scanner::get_scanner(const std::string& format)
{
#ifdef HAVE_GRIBAPI
    if (format == "grib" || format == "grib1" || format == "grib2")
        return std::unique_ptr<Scanner>(new scan::Grib);
#endif
#ifdef HAVE_DBALLE
    if (format == "bufr")
        return std::unique_ptr<Scanner>(new scan::Bufr);
#endif
#ifdef HAVE_HDF5
    if ((format == "h5") || (format == "odim") || (format == "odimh5"))
        return std::unique_ptr<Scanner>(new scan::OdimH5);
#endif
#ifdef HAVE_VM2
    if (format == "vm2")
        return std::unique_ptr<Scanner>(new scan::Vm2);
#endif
    throw std::runtime_error("No scanner available for format '" + format + "'");
}

const Validator& Scanner::get_validator(const std::string& format)
{
#ifdef HAVE_GRIBAPI
    if (format == "grib")
        return grib::validator();
#endif
#ifdef HAVE_DBALLE
    if (format == "bufr")
        return bufr::validator();
#endif
#ifdef HAVE_HDF5
    if (format == "odimh5")
        return odimh5::validator();
#endif
#ifdef HAVE_VM2
   if (format == "vm2")
       return vm2::validator();
#endif
    throw std::runtime_error("No validator available for format '" + format + "'");
}

std::string Scanner::normalise_format(const std::string& format, const char* default_format)
{
    std::string f = str::lower(format);
    if (f == "grib") return "grib";
    if (f == "grib1") return "grib";
    if (f == "grib2") return "grib";
    if (f == "bufr") return "bufr";
    if (f == "vm2") return "vm2";
#ifdef HAVE_HDF5
    if (f == "h5")     return "odimh5";
    if (f == "hdf5")   return "odimh5";
    if (f == "odim")   return "odimh5";
    if (f == "odimh5") return "odimh5";
#endif
    if (f == "yaml") return "yaml";
    if (f == "metadata") return "arkimet";
    if (default_format) return default_format;
    throw std::runtime_error("unsupported format `" + format + "`");
}

std::string Scanner::format_from_filename(const std::string& fname, const char* default_format)
{
    // Extract the extension
    size_t epos = fname.rfind('.');
    if (epos != string::npos)
        return normalise_format(str::lower(fname.substr(epos + 1)), default_format);
    else if (default_format)
        return default_format;
    else
    {
        stringstream ss;
        ss << "cannot auto-detect format from file name " << fname << ": file extension not recognised";
        throw std::runtime_error(ss.str());
    }
}

bool Scanner::update_sequence_number(const types::source::Blob& source, int& usn)
{
#ifdef HAVE_DBALLE
    // Update Sequence Numbers are only supported by BUFR
    if (source.format != "bufr")
        return false;

    auto data = source.read_data();
    string buf((const char*)data.data(), data.size());
    usn = Bufr::update_sequence_number(buf);
    return true;
#else
    return false;
#endif
}

bool Scanner::update_sequence_number(Metadata& md, int& usn)
{
#ifdef HAVE_DBALLE
    // Update Sequence Numbers are only supported by BUFR
    if (md.source().format != "bufr")
        return false;

    const auto& data = md.getData();
    string buf((const char*)data.data(), data.size());
    usn = Bufr::update_sequence_number(buf);
    return true;
#else
    return false;
#endif
}

std::vector<uint8_t> Scanner::reconstruct(const std::string& format, const Metadata& md, const std::string& value)
{
#ifdef HAVE_VM2
    if (format == "vm2")
    {
        return scan::Vm2::reconstruct(md, value);
    }
#endif
    throw runtime_error("cannot reconstruct " + format + " data: format not supported");
}

}
}
