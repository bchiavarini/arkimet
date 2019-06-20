#include "xargs.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/utils.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/utils/subprocess.h"
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::utils;

extern char **environ;

namespace arki {
namespace metadata {

Xargs::Xargs()
    : metadata::Clusterer(), tempfile(""), filename_argument(-1)
{
    const char* tmpdir = getenv("TMPDIR");

    if (tmpdir)
    {
        tempfile_template = str::joinpath(tmpdir, "arki-xargs.XXXXXX");
    } else {
        tempfile_template = "/tmp/arki-xargs.XXXXXX";
    }
}

Xargs::~Xargs()
{
}

void Xargs::start_batch(const std::string& new_format)
{
    metadata::Clusterer::start_batch(new_format);

    // Make a mutable copy of tempfile_template
    unique_ptr<char[]> tf(new char[tempfile_template.size() + 1]);
    memcpy(tf.get(), tempfile_template.c_str(), tempfile_template.size() + 1);
    tempfile = File::mkstemp(tf.get());
}

void Xargs::add_to_batch(Metadata& md)
{
    metadata::Clusterer::add_to_batch(md);
    md.stream_data(tempfile);
}

void Xargs::flush_batch()
{
    if (!tempfile.is_open()) return;
    int res;

    try {
        // Run process with fname as parameter
        res = run_child();
    } catch (...) {
        // Ignore close exceptions here, so we rethrow what really happened
        ::close(tempfile);
        ::unlink(tempfile.name().c_str());
        throw;
    }

    tempfile.close();
    sys::unlink_ifexists(tempfile.name());
    metadata::Clusterer::flush_batch();

    if (res != 0)
    {
        stringstream ss;
        ss << "cannot run " << command[0] << ": process returned exit status " << res;
        throw std::runtime_error(ss.str());
    }
}

int Xargs::run_child()
{
    if (count == 0) return 0;

    subprocess::Popen child({command[0]});
    child.set_stdin(subprocess::Redirect::DEVNULL);
    child.args = command;
    if (filename_argument == -1)
        child.args.push_back(tempfile.name());
    else {
        if ((unsigned)filename_argument < child.args.size())
            child.args[filename_argument] = tempfile.name();
    }

    // Import all the environment except ARKI_XARGS_* variables
    for (char** s = environ; *s; ++s)
    {
        string envstr(*s);
        if (str::startswith(envstr, "ARKI_XARGS_")) continue;
        child.env.push_back(envstr);
    }
    child.setenv("ARKI_XARGS_FILENAME", tempfile.name());
    child.setenv("ARKI_XARGS_FORMAT", str::upper(format));
    char buf[32];
    snprintf(buf, 32, "%zd", count);
    child.setenv("ARKI_XARGS_COUNT", buf);

    if (timespan_begin.get())
    {
        child.setenv("ARKI_XARGS_TIME_START", timespan_begin->to_iso8601(' '));
        if (timespan_end.get())
            child.setenv("ARKI_XARGS_TIME_END", timespan_end->to_iso8601(' '));
        else
            child.setenv("ARKI_XARGS_TIME_END", timespan_begin->to_iso8601(' '));
    }

    child.fork();
    return child.wait();
}

static size_t parse_interval(const std::string& str)
{
    string name = str::lower(str::strip(str));
    if (name == "minute") return 5;
    if (name == "hour") return 4;
    if (name == "day") return 3;
    if (name == "month") return 2;
    if (name == "year") return 1;
    throw std::runtime_error("cannot parse interval name: unsupported interval: " + str + ".  Valid intervals are minute, hour, day, month and year");
}

void Xargs::set_max_bytes(const std::string& val)
{
    max_bytes = parse_size(val);
}

void Xargs::set_interval(const std::string& val)
{
    max_interval = parse_interval(val);
}

}
}
