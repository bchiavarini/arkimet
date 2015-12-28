#include "arki/tests/tests.h"
#include "arki/scan/dir.h"
#include "arki/utils.h"
#include "arki/utils/sys.h"
#include "arki/wibble/exception.h"
#include <algorithm>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

struct arki_scan_dir_shar {
	arki_scan_dir_shar()
	{
	}
};
TESTGRP(arki_scan_dir);

// Test DirScanner on an empty directory
def_test(1)
{
	system("rm -rf dirscanner");
	mkdir("dirscanner", 0777);

	vector<string> ds = scan::dir("dirscanner");
	ensure(ds.empty());
}

// Test DirScanner on a populated directory
def_test(2)
{
	system("rm -rf dirscanner");
	mkdir("dirscanner", 0777);
	utils::createFlagfile("dirscanner/index.sqlite");
	mkdir("dirscanner/2007", 0777);
	mkdir("dirscanner/2008", 0777);
	utils::createFlagfile("dirscanner/2008/a.grib");
	utils::createFlagfile("dirscanner/2008/b.grib");
	mkdir("dirscanner/2008/temp", 0777);
	mkdir("dirscanner/2009", 0777);
	utils::createFlagfile("dirscanner/2009/a.grib");
	utils::createFlagfile("dirscanner/2009/b.grib");
	mkdir("dirscanner/2009/temp", 0777);
	mkdir("dirscanner/.archive", 0777);
	utils::createFlagfile("dirscanner/.archive/z.grib");

	vector<string> ds = scan::dir("dirscanner");
	sort(ds.begin(), ds.end());

	ensure_equals(ds.size(), 4u);
	ensure_equals(ds[0], "2008/a.grib");
	ensure_equals(ds[1], "2008/b.grib");
	ensure_equals(ds[2], "2009/a.grib");
	ensure_equals(ds[3], "2009/b.grib");
}

// Test file names interspersed with directory names
def_test(3)
{
	system("rm -rf dirscanner");
	mkdir("dirscanner", 0777);
	utils::createFlagfile("dirscanner/index.sqlite");
	mkdir("dirscanner/2008", 0777);
	utils::createFlagfile("dirscanner/2008/a.grib");
	utils::createFlagfile("dirscanner/2008/b.grib");
	mkdir("dirscanner/2008/a", 0777);
	utils::createFlagfile("dirscanner/2008/a/a.grib");
	mkdir("dirscanner/2009", 0777);
	utils::createFlagfile("dirscanner/2009/a.grib");
	utils::createFlagfile("dirscanner/2009/b.grib");

	vector<string> ds = scan::dir("dirscanner");
	sort(ds.begin(), ds.end());

	ensure_equals(ds.size(), 5u);
	ensure_equals(ds[0], "2008/a.grib");
	ensure_equals(ds[1], "2008/a/a.grib");
	ensure_equals(ds[2], "2008/b.grib");
	ensure_equals(ds[3], "2009/a.grib");
	ensure_equals(ds[4], "2009/b.grib");
}

}
