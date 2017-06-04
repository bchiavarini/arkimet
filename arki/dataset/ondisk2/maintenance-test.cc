#include "arki/dataset/tests.h"
#include "arki/dataset/maintenance-test.h"
#include "arki/dataset/reporter.h"
#include "arki/dataset/segmented.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/utils/sys.h"

using namespace arki;
using namespace arki::tests;
using namespace arki::utils;
using namespace arki::dataset;

namespace {

using namespace arki::dataset::maintenance_test;

class Tests : public MaintenanceTest
{
    using MaintenanceTest::MaintenanceTest;

    void require_rescan() override
    {
        throw std::runtime_error("require_rescan not available for ondisk2");
    }

    void register_tests() override;

    /**
     * ondisk2 datasets can store overlaps even on dir segments, because the
     * index sadly lacks a unique constraint on (file, offset)
     */
    bool can_detect_overlap() const override { return true; }
};

void Tests::register_tests()
{
    MaintenanceTest::register_tests();
}

Tests test_ondisk2_plain("arki_dataset_ondisk2_maintenance", MaintenanceTest::SEGMENT_CONCAT, "type=ondisk2\n");
Tests test_ondisk2_plain_dir("arki_dataset_ondisk2_maintenance_dirs", MaintenanceTest::SEGMENT_DIR, "type=ondisk2\nsegments=dir\n");

}

