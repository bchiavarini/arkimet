/*
 * utils - General utility functions
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <arki/utils.h>
#include <wibble/sys/fs.h>
#include <wibble/sys/process.h>

#include <fstream>
#include <cctype>
#include <cstring>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include "config.h"

using namespace std;
using namespace wibble;

namespace arki {
namespace utils {

bool isdir(const std::string& root, wibble::sys::fs::Directory::const_iterator& i)
{
#ifdef HAVE_STRUCT_DIRENT_D_TYPE
	if (i->d_type == DT_DIR)
		return true;
	if (i->d_type != DT_UNKNOWN)
		return false;
#endif
	// No d_type, we'll need to stat
	std::auto_ptr<struct stat> st = wibble::sys::fs::stat(wibble::str::joinpath(root, *i));
	if (st.get() == 0)
		return false;
	if (S_ISDIR(st->st_mode))
		return true;
    return false;
}

bool isdir(const std::string& pathname)
{
	std::auto_ptr<struct stat> st = wibble::sys::fs::stat(pathname);
	if (st.get() == 0)
		return false;
	return S_ISDIR(st->st_mode);
}

std::string readFile(const std::string& filename)
{
	if (filename == "-")
		return readFile(cin, filename);
	else
	{
		ifstream input(filename.c_str(), ios::in);
		if (!input.is_open() || input.fail())
			throw wibble::exception::File(filename, "opening file for reading");
		return readFile(input, filename);
	}
}

std::string readFile(std::istream& input, const std::string& filename)
{
	static const size_t bufsize = 4096;
	char buf[bufsize];
	string res;
	while (true)
	{
		input.read(buf, bufsize);
		res += string(buf, input.gcount());
		if (input.eof())
			break;
		if (input.fail())
			throw wibble::exception::File(filename, "reading data");
	}
	return res;
}

void createFlagfile(const std::string& pathname)
{
	int fd = ::open(pathname.c_str(), O_WRONLY | O_CREAT | O_NOCTTY, 0666);
	if (fd == -1)
		throw wibble::exception::File(pathname, "opening/creating file");
	::close(fd);
}

void createNewFlagfile(const std::string& pathname)
{
	int fd = ::open(pathname.c_str(), O_WRONLY | O_CREAT | O_NOCTTY | O_EXCL, 0666);
	if (fd == -1)
		throw wibble::exception::File(pathname, "creating file");
	::close(fd);
}

void removeFlagfile(const std::string& pathname)
{
	if (unlink(pathname.c_str()) != 0)
		if (errno != ENOENT)
			throw wibble::exception::File(pathname, "removing file");
}

bool hasFlagfile(const std::string& pathname)
{
	return wibble::sys::fs::access(pathname, F_OK);
}

void hexdump(const char* name, const std::string& str)
{
	fprintf(stderr, "%s ", name);
	for (string::const_iterator i = str.begin(); i != str.end(); ++i)
	{
		fprintf(stderr, "%02x ", (int)(unsigned char)*i);
	}
	fprintf(stderr, "\n");
}

void hexdump(const char* name, const unsigned char* str, int len)
{
	fprintf(stderr, "%s ", name);
	for (int i = 0; i < len; ++i)
	{
		fprintf(stderr, "%02x ", str[i]);
	}
	fprintf(stderr, "\n");
}

void rmtree(const std::string& dir)
{
    sys::fs::Directory d(dir);
    for (sys::fs::Directory::const_iterator i = d.begin();
            i != d.end(); ++i)
    {
        if (*i == "." || *i == "..") continue;
        string pathname = str::joinpath(dir, *i);
        if (i->d_type == DT_DIR ||
            (i->d_type == DT_UNKNOWN && sys::fs::isDirectory(pathname)))
        {
            rmtree(pathname);
        } else {
            if (unlink(pathname.c_str()) < 0)
                throw wibble::exception::System("cannot delete " + pathname);
        }
    }
    if (rmdir(dir.c_str()) < 0)
        throw wibble::exception::System("cannot delete directory " + dir);
}

MoveToTempDir::MoveToTempDir(const std::string& pattern)
{
    old_dir = sys::process::getcwd();
    char buf[pattern.size() + 1];
    memcpy(buf, pattern.c_str(), pattern.size() + 1);
    if (mkdtemp(buf) == NULL)
        throw wibble::exception::System("cannot create temporary directory");
    tmp_dir = buf;
    sys::process::chdir(tmp_dir);
}

MoveToTempDir::~MoveToTempDir()
{
    sys::process::chdir(old_dir);
    rmtree(tmp_dir);
}

}
}
// vim:set ts=4 sw=4:
