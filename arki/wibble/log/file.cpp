#include "arki/wibble/sys/filelock.h"
#include "arki/wibble/log/file.h"
#include "arki/exceptions.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

using namespace arki;

namespace wibble {
namespace log {

FileSender::FileSender(const std::string& filename) : out(-1), name(filename)
{
    out = open(filename.c_str(), O_WRONLY | O_CREAT, 0666);
    if (out < 0)
        throw_file_error(filename, "cannot open logfile for append");
}

FileSender::~FileSender()
{
    if (out >= 0) close(out);
}

void FileSender::send(Level level, const std::string& msg)
{
#ifndef __gnu_hurd__
    // Write it all in a single write(2) so multiple processes can log to
    // the same file
    sys::fs::FileLock lock(out, F_WRLCK);
#endif

    // Seek to end of file
    if (lseek(out, 0, SEEK_END) < 0)
        throw_file_error(name, "cannot move to end of file");

    // Write it all out
    size_t done = 0;
    while (done < msg.size())
    {
        ssize_t res = write(out, msg.data() + done, msg.size() - done);
        if (res < 0)
            throw_file_error(name, "cannot write to file");
        done += res;
    }

    // Write trailing newline
    while (true)
    {
        ssize_t res = write(out, "\n", 1);
        if (res < 0)
            throw_file_error(name, "cannot write to file");
        if (res > 0)
            break;
    }
}

}
}
