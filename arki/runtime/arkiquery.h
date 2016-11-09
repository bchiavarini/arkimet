#ifndef ARKI_RUNTIME_ARKIQUERY_H
#define ARKI_RUNTIME_ARKIQUERY_H

#include <arki/runtime.h>
#include <string>

namespace arki {
namespace runtime {

struct ArkiQuery : public runtime::ArkiTool
{
    std::string qmacro;
    bool merged = false;

    void set_query(const std::string& strquery) override;

    int main() override;
};

}
}

#endif
