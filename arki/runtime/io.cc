/*
 * runtime - Common code used in most xgribarch executables
 *
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/runtime/io.h>

#include <wibble/exception.h>
#include <wibble/string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace std;
using namespace wibble;
using namespace wibble::commandline;

namespace arki {
namespace runtime {

Input::Input(wibble::commandline::Parser& opts)
	: m_in(&cin), m_name("(stdin)")
{
	if (opts.hasNext())
	{
		string file = opts.next();
		if (file != "-")
		{
			m_file_in.open(file.c_str(), ios::in);
			if (!m_file_in.is_open() || m_file_in.fail())
				throw wibble::exception::File(file, "opening file for reading");
			m_in = &m_file_in;
			m_name = file;
		}
	}
}

Input::Input(const std::string& file)
	: m_in(&cin), m_name("(stdin)")
{
	if (file != "-")
	{
		m_file_in.open(file.c_str(), ios::in);
		if (!m_file_in.is_open() || m_file_in.fail())
			throw wibble::exception::File(file, "opening file for reading");
		m_in = &m_file_in;
		m_name = file;
	}
}

Output::Output() : m_out(0) {}

Output::Output(const std::string& fileName) : m_out(0)
{
	openFile(fileName);
}

Output::Output(wibble::commandline::Parser& opts) : m_out(0)
{
	if (opts.hasNext())
	{
		string file = opts.next();
		if (file != "-")
			openFile(file);
	}
	if (!m_out)
		openStdout();
}

Output::Output(wibble::commandline::StringOption& opt) : m_out(0)
{
	if (opt.isSet())
	{
		string file = opt.value();
		if (file != "-")
			openFile(file);
	}
	if (!m_out)
		openStdout();
}

Output::~Output()
{
	if (m_out) delete m_out;
}

void Output::closeCurrent()
{
	if (!m_out) return;
	posixBuf.sync();
	int fd = posixBuf.detach();
	if (fd != 1)
		::close(fd);
	delete m_out;
	m_out = 0;
}

void Output::openStdout()
{
	if (m_name == "-") return;
	closeCurrent();
	m_name = "-";
	posixBuf.attach(1);
	m_out = new ostream(&posixBuf);
}

void Output::openFile(const std::string& fname, bool append)
{
	if (m_name == fname) return;
	closeCurrent();
	int fd;
	if (append)
		fd = open(fname.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0666);
	else
		fd = open(fname.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
	if (fd == -1)
		throw wibble::exception::File(fname, "opening file for writing");
	m_name = fname;
	posixBuf.attach(fd);
	m_out = new ostream(&posixBuf);
}

}
}
// vim:set ts=4 sw=4:
