#include "arki/runtime/arki-dump.h"
#include "arki-dump.h"
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"
#include "common.h"

using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_ArkiDump_Type = nullptr;

}


namespace {

struct run_ : public MethKwargs<run_, arkipy_ArkiDump>
{
    constexpr static const char* name = "run";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "int";
    constexpr static const char* summary = "run arki-dump";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "args", nullptr };
        PyObject* pyargs = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", const_cast<char**>(kwlist), &pyargs))
            return nullptr;

        if (!PySequence_Check(pyargs))
        {
            PyErr_SetString(PyExc_TypeError, "args argument must be a sequence of strings");
            throw PythonException();
        }

        // Unpack a sequence of strings into argc/argv
        unsigned argc = PySequence_Size(pyargs);
        std::vector<const char*> argv;
        argv.reserve(argc + 1);
        for (unsigned i = 0; i < argc; ++i)
        {
            pyo_unique_ptr o(throw_ifnull(PySequence_ITEM(pyargs, i)));
            argv.emplace_back(from_python<const char*>(o));
        }
        argv.emplace_back(nullptr);

        try {
            int res;
            {
                ReleaseGIL rg;
                res = self->arki_dump->run(argc, argv.data());
            }
            return throw_ifnull(PyLong_FromLong(res));
        } ARKI_CATCH_RETURN_PYO
    }
};


struct ArkiDumpDef : public Type<ArkiDumpDef, arkipy_ArkiDump>
{
    constexpr static const char* name = "ArkiDump";
    constexpr static const char* qual_name = "arkimet.ArkiDump";
    constexpr static const char* doc = R"(
arki-dump implementation
)";
    GetSetters<> getsetters;
    Methods<run_> methods;

    static void _dealloc(Impl* self)
    {
        delete self->arki_dump;
        Py_TYPE(self)->tp_free(self);
    }

    static PyObject* _str(Impl* self)
    {
        return PyUnicode_FromString(name);
    }

    static PyObject* _repr(Impl* self)
    {
        std::string res = qual_name;
        res += " object";
        return PyUnicode_FromString(res.c_str());
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { nullptr };
        if (!PyArg_ParseTupleAndKeywords(args, kw, "", const_cast<char**>(kwlist)))
            return -1;

        try {
            self->arki_dump = new arki::runtime::ArkiDump;
        } ARKI_CATCH_RETURN_INT

        return 0;
    }
};

ArkiDumpDef* arki_dump_def = nullptr;


}

namespace arki {
namespace python {

void register_arki_dump(PyObject* m)
{
    arki_dump_def = new ArkiDumpDef;
    arki_dump_def->define(arkipy_ArkiDump_Type, m);

}

}
}
