#ifndef ARKI_RUNTIME_TESTS_H
#define ARKI_RUNTIME_TESTS_H

#include <arki/file.h>
#include <initializer_list>

namespace arki {
namespace runtime {
namespace tests {

struct CatchOutput
{
    arki::utils::sys::File file_stdin;
    arki::utils::sys::File file_stdout;
    arki::utils::sys::File file_stderr;
    int orig_stdin;
    int orig_stdout;
    int orig_stderr;

    CatchOutput();
    ~CatchOutput();

    int save(int src, int tgt);
    void restore(int src, int tgt);
};


typedef int (*cmdline_func)(int argc, const char* argv[]);

int run_cmdline(cmdline_func func, std::initializer_list<const char*> args);

}
}
}

#endif
