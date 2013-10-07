/*
 * Copyright (C) 2007--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "config.h"

#include "arki/tests/tests.h"
#include "arki/metadata/tests.h"
#include "arki/metadata/collection.h"
#include "arki/data.h"
#include "arki/data/lines.h"
#include "arki/scan/any.h"
#include "arki/utils/files.h"
#include <sstream>

namespace std {
static inline std::ostream& operator<<(std::ostream& o, const arki::Metadata& m)
{
        m.writeYaml(o);
            return o;
}
}

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::data;
using namespace arki::utils;
using namespace wibble;
using namespace wibble::tests;

struct arki_data_lines_shar {
    string fname;
    metadata::Collection mdc;

    arki_data_lines_shar()
        : fname("testfile.grib")
    {
        system(("rm -f " + fname).c_str());
        // Keep some valid metadata handy
        ensure(scan::scan("inbound/test.grib1", mdc));
    }

    /**
     * Create a writer
     * return the data::Writer so that we manage the writer lifetime, but also
     * the underlying implementation so we can test it.
     */
    data::Writer make_w(const std::string& fname, lines::Writer*& w)
    {
        data::Writer res = data::Writer::get("vm2", fname);

        // Access and upcast the underlying implementation
        w = dynamic_cast<lines::Writer*>(res._implementation());

        if (!w)
            throw wibble::exception::Consistency("creating test writer", "the writer we got in not a lines::Writer");

        return res;
    }
};

TESTGRP(arki_data_lines);

namespace {

inline size_t datasize(const Metadata& md)
{
    return md.source.upcast<types::source::Blob>()->size;
}

void test_append_transaction_ok(WIBBLE_TEST_LOCPRM, data::Writer dw, Metadata& md)
{
    typedef types::source::Blob Blob;

    // Make a snapshot of everything before appending
    Item<types::Source> orig_source = md.source;
    size_t data_size = datasize(md);
    size_t orig_fsize = files::size(dw.fname());

    // Start the append transaction, nothing happens until commit
    off_t ofs;
    Pending p = dw.append(md, &ofs);
    wassert(actual((size_t)ofs) == orig_fsize);
    wassert(actual(files::size(dw.fname())) == orig_fsize);
    wassert(actual(md.source) == orig_source);

    // Commit
    p.commit();

    // After commit, data is appended
    wassert(actual(files::size(dw.fname())) == orig_fsize + data_size + 1);

    // And metadata is updated
    UItem<Blob> s = md.source.upcast<Blob>();
    wassert(actual(s->format) == "grib1");
    wassert(actual(s->offset) == orig_fsize);
    wassert(actual(s->size) == data_size);
    wassert(actual(s->filename) == dw.fname());
}

void test_append_transaction_rollback(WIBBLE_TEST_LOCPRM, data::Writer dw, Metadata& md)
{
    // Make a snapshot of everything before appending
    Item<types::Source> orig_source = md.source;
    size_t orig_fsize = files::size(dw.fname());

    // Start the append transaction, nothing happens until commit
    off_t ofs;
    Pending p = dw.append(md, &ofs);
    wassert(actual((size_t)ofs) == orig_fsize);
    wassert(actual(files::size(dw.fname())) == orig_fsize);
    wassert(actual(md.source) == orig_source);

    // Rollback
    p.rollback();

    // After rollback, nothing has changed
    wassert(actual(files::size(dw.fname())) == orig_fsize);
    wassert(actual(md.source) == orig_source);
}

}

// Try to append some data
template<> template<>
void to::test<1>()
{
    wassert(!actual(fname).fileexists());
    {
        lines::Writer* w;
        data::Writer dw = make_w(fname, w);

        // It should exist but be empty
        wassert(actual(fname).fileexists());
        wassert(actual(files::size(fname)) == 0u);

        // Try a successful transaction
        wruntest(test_append_transaction_ok, dw, mdc[0]);

        // Then fail one
        wruntest(test_append_transaction_rollback, dw, mdc[1]);

        // Then succeed again
        wruntest(test_append_transaction_ok, dw, mdc[2]);
    }

    // Data writer goes out of scope, file is closed and flushed
    metadata::Collection mdc1;

    // Scan the file we created
    wassert(actual(scan::scan(fname, mdc1)).istrue());

    // Check that it only contains the 1st and 3rd data
    wassert(actual(mdc1.size()) == 2u);
    wassert(actual(mdc1[0]).is_similar(mdc[0]));
    wassert(actual(mdc1[1]).is_similar(mdc[2]));
}

// Test with large files
template<> template<>
void to::test<2>()
{
    {
        lines::Writer* w;
        data::Writer dw = make_w(fname, w);

        // Make a file that looks HUGE, so that appending will make its size
        // not fit in a 32bit off_t
        w->truncate(0x7FFFFFFF);
        wassert(actual(files::size(fname)) == 0x7FFFFFFFu);

        // Try a successful transaction
        wruntest(test_append_transaction_ok, dw, mdc[0]);

        // Then fail one
        wruntest(test_append_transaction_rollback, dw, mdc[1]);

        // Then succeed again
        wruntest(test_append_transaction_ok, dw, mdc[2]);
    }

    wassert(actual(files::size(fname)) == 0x7FFFFFFFu + datasize(mdc[0]) + datasize(mdc[2]) + 2);

    // Won't attempt rescanning, as the grib reading library will have to
    // process gigabytes of zeros
}

// Test stream writer
template<> template<>
void to::test<3>()
{
    std::stringstream out;
    const OstreamWriter* w = OstreamWriter::get("vm2");
    w->stream(mdc[0], out);
    wassert(actual(out.str().size()) == datasize(mdc[0]) + 1);
}

// Test raw append
template<> template<>
void to::test<4>()
{
    wassert(!actual(fname).fileexists());
    {
        lines::Writer* w;
        data::Writer dw = make_w(fname, w);
        sys::Buffer buf("ciao", 4);
        ensure_equals(dw.append(buf), 0);
        ensure_equals(dw.append(buf), 5);
    }

    wassert(actual(utils::files::read_file(fname)) == "ciao\nciao\n");
}

// Test Info
template<> template<>
void to::test<5>()
{
    const Info* i = Info::get("vm2");
    off64_t a = 10;
    size_t b = 20;
    i->raw_to_wrapped(a, b);
    wassert(actual(a) == 10);
    wassert(actual(b) == 22);
}

}

