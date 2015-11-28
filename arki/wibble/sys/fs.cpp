#include <arki/wibble/sys/fs.h>
#include <fstream>
#include <dirent.h> // opendir, closedir
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <malloc.h> // alloca on win32 seems to live there

namespace wibble {
namespace sys {
namespace fs {

bool access(const std::string &s, int m)
{
	return ::access(s.c_str(), m) == 0;
}

bool exists(const std::string& file)
{
    return sys::fs::access(file, F_OK);
}


}
}
}

// vim:set ts=4 sw=4: