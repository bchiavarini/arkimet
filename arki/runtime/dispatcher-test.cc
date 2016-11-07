#include "config.h"
#include "dispatcher.h"
#include "arki/dataset/tests.h"

using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::runtime;
using namespace arki::utils;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
} test("arki_runtime_dispatcher");

void Tests::register_tests() {

// Export only binary metadata (the default)
add_method("empty", [] {
});

}

}
