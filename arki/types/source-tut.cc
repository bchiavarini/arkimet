#include <arki/types/tests.h>
#include <arki/types/source.h>
#include <arki/types/source/blob.h>
#include <arki/emitter/json.h>
#include <arki/emitter/memory.h>
#include <arki/binary.h>
#include <arki/wibble/sys/process.h>

namespace tut {
using namespace std;
using namespace arki::tests;
using namespace wibble::sys::process;
using namespace arki;
using namespace arki::types;

struct arki_types_source_shar {
};
TESTGRP(arki_types_source);

// Check Blob
template<> template<>
void to::test<1>()
{
    unique_ptr<Source> o = Source::createBlob("test", "", "testfile", 21, 42);
    wassert(actual(o).is_source_blob("test", "", "testfile", 21u, 42u));

	#if 0
	source.prependPath("/pippo");
	source.getBlob(fname, offset, size);
	ensure_equals(fname, "/pippo/testfile");
	ensure_equals(offset, 21u);
	ensure_equals(size, 42u);
	#endif

    wassert(actual(o) == Source::createBlob("test", "", "testfile", 21, 42));
    wassert(actual(o) == Source::createBlob("test", "/tmp", "testfile", 21, 42));
    wassert(actual(o) != Source::createBlob("test", "", "testfile", 22, 43));
    wassert(actual(o) != Source::createURL("test", "testfile"));
    wassert(actual(o) != Source::createInline("test", 43));

    // Test encoding/decoding
    wassert(actual(o).serializes());
}

// Check URL
template<> template<>
void to::test<2>()
{
    unique_ptr<Source> o = Source::createURL("test", "http://foobar");
    wassert(actual(o).is_source_url("test", "http://foobar"));

    wassert(actual(o) == Source::createURL("test", "http://foobar"));
    wassert(actual(o) != Source::createURL("test", "http://foobar/a"));
    wassert(actual(o) != Source::createBlob("test", "", "http://foobar", 1, 2));
    wassert(actual(o) != Source::createInline("test", 1));

    // Test encoding/decoding
    wassert(actual(o).serializes());
}

// Check Inline
template<> template<>
void to::test<3>()
{
    unique_ptr<Source> o = Source::createInline("test", 12345);
    wassert(actual(o).is_source_inline("test", 12345u));

    wassert(actual(o) == Source::createInline("test", 12345));
    wassert(actual(o) != Source::createInline("test", 12344));
    wassert(actual(o) != Source::createBlob("test", "", "", 0, 12344));
    wassert(actual(o) != Source::createURL("test", ""));

    // Test encoding/decoding
    wassert(actual(o).serializes());
}

// Check Blob on big files
template<> template<>
void to::test<4>()
{
    unique_ptr<Source> o = Source::createBlob("test", "", "testfile", 0x8000FFFFffffFFFFLLU, 42);
    wassert(actual(o).is_source_blob("test", "", "testfile", 0x8000FFFFffffFFFFLLU, 42u));

    // Test encoding/decoding
    wassert(actual(o).serializes());
}

// Check Blob and pathnames handling
template<> template<>
void to::test<5>()
{
    unique_ptr<source::Blob> o = source::Blob::create("test", "", "testfile", 21, 42);
    wassert(actual(o->absolutePathname()) == "testfile");

    o = source::Blob::create("test", "/tmp", "testfile", 21, 42);
    wassert(actual(o->absolutePathname()) == "/tmp/testfile");

    o = source::Blob::create("test", "/tmp", "/antani/testfile", 21, 42);
    wassert(actual(o->absolutePathname()) == "/antani/testfile");
}

// Check Blob and pathnames handling in serialization
template<> template<>
void to::test<6>()
{
    unique_ptr<Source> o = Source::createBlob("test", "/tmp", "testfile", 21, 42);

    // Encode to binary, decode, we lose the root
    vector<uint8_t> enc = o->encodeBinary();
    BinaryDecoder dec(enc);
    unique_ptr<Source> decoded = downcast<Source>(types::decode(dec));
    wassert(actual(decoded).is_source_blob("test", "", "testfile", 21, 42));

    // Encode to YAML, decode, basedir and filename have merged
    stringstream tmp;
    tmp << *o;
    string enc1 = tmp.str();
    decoded = types::Source::decodeString(enc1);
    wassert(actual(decoded).is_source_blob("test", "", "/tmp/testfile", 21, 42));

    // Encode to JSON, decode, we keep the root
    {
        std::stringstream jbuf;
        emitter::JSON json(jbuf);
        o->serialise(json);
        jbuf.seekg(0);
        emitter::Memory parsed;
        emitter::JSON::parse(jbuf, parsed);

        decoded = downcast<Source>(types::decodeMapping(parsed.root().get_mapping()));
        wassert(actual(decoded).is_source_blob("test", "/tmp", "testfile", 21, 42));
    }
}

// Test basic type ops
template<> template<>
void to::test<7>()
{
    arki::tests::TestGenericType tblob("source", "BLOB(bufr,testfile:100+50)");
    tblob.lower.push_back("BLOB(aaa,testfile:100+50)");
    tblob.lower.push_back("BLOB(bufr,festfile:100+50)");
    tblob.lower.push_back("BLOB(bufr,testfile:99+50)");
    tblob.lower.push_back("BLOB(bufr,testfile:100+49)");
    tblob.higher.push_back("BLOB(grib,testfile:100+50)");
    tblob.higher.push_back("BLOB(bufr,zestfile:100+50)");
    tblob.higher.push_back("BLOB(bufr,testfile:101+50)");
    tblob.higher.push_back("BLOB(bufr,testfile:100+51)");
    tblob.higher.push_back("INLINE(bufr,9)");
    tblob.higher.push_back("URL(bufr,foo)");
    wassert(tblob.check());

    arki::tests::TestGenericType tinline("source", "INLINE(bufr,10)");
    tinline.lower.push_back("INLINE(aaa, 11)");
    tinline.lower.push_back("INLINE(bufr, 9)");
    tinline.higher.push_back("INLINE(bufr, 11)");
    tinline.higher.push_back("INLINE(ccc, 9)");
    wassert(tinline.check());

    arki::tests::TestGenericType turl("source", "URL(bufr,foo)");
    turl.lower.push_back("URL(aaa,zoo)");
    turl.lower.push_back("URL(bufr,boo)");
    turl.higher.push_back("URL(bufr,zoo)");
    turl.higher.push_back("URL(ccc,foo)");
    wassert(turl.check());
}

}
