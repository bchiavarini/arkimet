#include "config.h"
#include <arki/dataset/http/test-utils.h>
#include <arki/utils/net/http.h>
#include <arki/utils/net/mime.h>
#include <arki/exceptions.h>
#include <cstring>
#include <cstdlib>
#include <unistd.h>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace tests {

FakeRequest::FakeRequest(const std::string& tmpfname)
    : fname(0), fd(-1), response_offset(0)
{
    fname = new char[tmpfname.size() + 1];
    strncpy(fname, tmpfname.c_str(), tmpfname.size() + 1);
    fd = mkstemp(fname);
    if (fd == -1)
        throw_file_error(fname, "cannot create temporary file");
    unlink(fname);
}
FakeRequest::~FakeRequest()
{
    if (fd != -1) close(fd);
    if (fname) delete[] fname;
}

void FakeRequest::write(const std::string& str)
{
    size_t pos = 0;
    while (pos < str.size())
    {
        ssize_t res = ::write(fd, str.data() + pos, str.size() - pos);
        if (res < 0)
            throw_file_error(fname, "cannot write to file");
        pos += (size_t)res;
    }
    response_offset += str.size();
}

void FakeRequest::write_get(const std::string& query)
{
    write("GET " + query + " HTTP/1.1\r\n");
    write("User-Agent: arkimet-fakequery\r\n");
    write("Host: localhost:0\r\n");
    write("Accept: */*\r\n");
    write("\r\n");
}

void FakeRequest::reset()
{
    if (lseek(fd, 0, SEEK_SET) < 0)
        throw_file_error(fname, "cannot seek to start of file");
}

void FakeRequest::setup_request(net::http::Request& req)
{
    reset();

    req.sock = fd;
    req.peer_hostname = "localhost";
    req.peer_hostaddr = "127.0.0.1";
    req.peer_port = "0";
    req.server_software = "arkimet-test/0.0";
    req.server_name = "http://localhost:0";
    req.server_port = "0";

    req.read_request();
}

void FakeRequest::read_response()
{
    if (lseek(fd, response_offset, SEEK_SET) < 0)
        throw_file_error(fname, "cannot seek to start of response in file");

    // Read headers
    net::mime::Reader reader;
    if (!reader.read_line(fd, response_method))
        throw std::runtime_error("cannot read response method: no headers found");
    if (!reader.read_headers(fd, response_headers))
        throw std::runtime_error("cannot read response body: no body found");

    // Read the rest as reponse body
    while (true)
    {
        uint8_t buf[4096];
        ssize_t res = read(fd, buf, 4096);
        if (res < 0)
            throw_file_error(fname, "cannot read response body");
        if (res == 0) break;
        append_body(buf, res);
    }
}

void FakeRequest::dump_headers(std::ostream& out)
{
    for (map<string, string>::const_iterator i = response_headers.begin();
            i != response_headers.end(); ++i)
        out << "header " << i->first << ": " << i->second << endl;
}

void StringFakeRequest::append_body(const uint8_t* buf, size_t len)
{
    response_body.append((const char*)buf, len);
}

void BufferFakeRequest::append_body(const uint8_t* buf, size_t len)
{
    response_body.insert(response_body.end(), buf, buf + len);
}

}
}
