#include "cfg.h"
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"
#include "arki/utils/files.h"
#include "arki/core/file.h"
#include "common.h"
#include "files.h"

using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_cfgSections_Type = nullptr;
PyTypeObject* arkipy_cfgSection_Type = nullptr;

}


namespace {

struct section : public MethKwargs<section, arkipy_cfgSections>
{
    constexpr static const char* name = "section";
    constexpr static const char* signature = "Str";
    constexpr static const char* returns = "Optional[arki.cfg.Section]";
    constexpr static const char* summary = "return the named section, if it exists, or None if it does not";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "name", nullptr };
        const char* arg_name = nullptr;
        Py_ssize_t arg_name_len;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "s#", const_cast<char**>(kwlist), &arg_name, &arg_name_len))
            return nullptr;

        try {
            std::string name(arg_name, arg_name_len);
            arki::core::cfg::Section* res = self->sections.section(name);
            if (!res)
                Py_RETURN_NONE;
            return cfg_section_reference((PyObject*)self, res);
        } ARKI_CATCH_RETURN_PYO
    }
};

struct obtain : public MethKwargs<obtain, arkipy_cfgSections>
{
    constexpr static const char* name = "obtain";
    constexpr static const char* signature = "Str";
    constexpr static const char* returns = "arki.cfg.Section";
    constexpr static const char* summary = "return the named section, creating it if it does not exist";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "name", nullptr };
        const char* arg_name = nullptr;
        Py_ssize_t arg_name_len;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "s#", const_cast<char**>(kwlist), &arg_name, &arg_name_len))
            return nullptr;

        try {
            std::string name(arg_name, arg_name_len);
            arki::core::cfg::Section& res = self->sections.obtain(name);
            return cfg_section_reference((PyObject*)self, &res);
        } ARKI_CATCH_RETURN_PYO
    }
};

struct sections_get : public MethKwargs<sections_get, arkipy_cfgSections>
{
    constexpr static const char* name = "get";
    constexpr static const char* signature = "name: str, default: Optional[Any]=None";
    constexpr static const char* returns = "Union[arki.cfg.Section, Any]";
    constexpr static const char* summary = "return the named section, or the given default value if it does not exist";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "name", "default", nullptr };
        const char* arg_name = nullptr;
        Py_ssize_t arg_name_len;
        PyObject* arg_default = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "s#|O", const_cast<char**>(kwlist), &arg_name, &arg_name_len, &arg_default))
            return nullptr;

        try {
            std::string name(arg_name, arg_name_len);
            arki::core::cfg::Section* res = self->sections.section(name);
            if (res)
                return cfg_section_reference((PyObject*)self, res);
            if (!arg_default)
                Py_RETURN_NONE;
            else
            {
                Py_INCREF(arg_default);
                return arg_default;
            }
        } ARKI_CATCH_RETURN_PYO
    }
};

struct sections_keys : public MethNoargs<sections_keys, arkipy_cfgSections>
{
    constexpr static const char* name = "keys";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "Iterable[str]";
    constexpr static const char* summary = "Iterate over section names";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self)
    {
        try {
            pyo_unique_ptr res(throw_ifnull(PyTuple_New(self->sections.size())));
            unsigned pos = 0;
            for (auto& si: self->sections)
            {
                pyo_unique_ptr key(to_python(si.first));
                PyTuple_SET_ITEM(res.get(), pos, key.release());
                ++pos;
            }
            return res.release();
        } ARKI_CATCH_RETURN_PYO
    }
};

struct sections_items : public MethNoargs<sections_items, arkipy_cfgSections>
{
    constexpr static const char* name = "items";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "Iterable[Tuple[str, arki.cfg.Section]]";
    constexpr static const char* summary = "Iterate over section names and sections";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self)
    {
        try {
            pyo_unique_ptr res(throw_ifnull(PyTuple_New(self->sections.size())));
            unsigned pos = 0;
            for (auto& si: self->sections)
            {
                pyo_unique_ptr key(to_python(si.first));
                pyo_unique_ptr val(cfg_section_reference((PyObject*)self, &si.second));
                pyo_unique_ptr pair(throw_ifnull(PyTuple_Pack(2, key.get(), val.get())));
                PyTuple_SET_ITEM(res.get(), pos, pair.release());
                ++pos;
            }
            return res.release();
        } ARKI_CATCH_RETURN_PYO
    }
};


struct section_get : public MethKwargs<section_get, arkipy_cfgSection>
{
    constexpr static const char* name = "get";
    constexpr static const char* signature = "name: str, default: Optional[Any]=None";
    constexpr static const char* returns = "Union[str, Any]";
    constexpr static const char* summary = "return the value for the given key, or the given default value if it does not exist";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "name", "default", nullptr };
        const char* arg_name = nullptr;
        Py_ssize_t arg_name_len;
        PyObject* arg_default = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "s#|O", const_cast<char**>(kwlist), &arg_name, &arg_name_len, &arg_default))
            return nullptr;

        try {
            std::string name(arg_name, arg_name_len);
            auto i = self->section->find(name);
            if (i != self->section->end())
                return to_python(i->second);
            if (!arg_default)
                Py_RETURN_NONE;
            else
            {
                Py_INCREF(arg_default);
                return arg_default;
            }
        } ARKI_CATCH_RETURN_PYO
    }
};
struct section_keys : public MethNoargs<section_keys, arkipy_cfgSection>
{
    constexpr static const char* name = "keys";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "Iterable[str]";
    constexpr static const char* summary = "Iterate over key names";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self)
    {
        try {
            pyo_unique_ptr res(throw_ifnull(PyTuple_New(self->section->size())));
            unsigned pos = 0;
            for (auto& si: *self->section)
            {
                pyo_unique_ptr key(to_python(si.first));
                PyTuple_SET_ITEM(res.get(), pos, key.release());
                ++pos;
            }
            return res.release();
        } ARKI_CATCH_RETURN_PYO
    }
};

struct section_items : public MethNoargs<section_items, arkipy_cfgSection>
{
    constexpr static const char* name = "items";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "Iterable[Tuple[str, str]]";
    constexpr static const char* summary = "Iterate over key/value pairs";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self)
    {
        try {
            pyo_unique_ptr res(throw_ifnull(PyTuple_New(self->section->size())));
            unsigned pos = 0;
            for (auto& si: *self->section)
            {
                pyo_unique_ptr key(to_python(si.first));
                pyo_unique_ptr val(to_python(si.second));
                pyo_unique_ptr pair(throw_ifnull(PyTuple_Pack(2, key.get(), val.get())));
                PyTuple_SET_ITEM(res.get(), pos, pair.release());
                ++pos;
            }
            return res.release();
        } ARKI_CATCH_RETURN_PYO
    }
};


struct parse_sections : public ClassMethKwargs<parse_sections>
{
    constexpr static const char* name = "parse";
    constexpr static const char* signature = "Union[Str, TextIO]";
    constexpr static const char* returns = "arki.cfg.Sections";
    constexpr static const char* summary = "parse the named file or open file, and return the resulting Sections object";
    constexpr static const char* doc = nullptr;

    static PyObject* run(PyTypeObject* cls, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "input", nullptr };
        PyObject* arg_input = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", const_cast<char**>(kwlist), &arg_input))
            return nullptr;

        try {
            if (PyUnicode_Check(arg_input))
            {
                std::string fname = from_python<std::string>(arg_input);
                arki::utils::sys::File in(fname, O_RDONLY);
                auto parsed = arki::core::cfg::Sections::parse(in);
                // If string, open file and parse it
                py_unique_ptr<arkipy_cfgSections> res(throw_ifnull(PyObject_New(arkipy_cfgSections, arkipy_cfgSections_Type)));
                new (&(res->sections)) arki::core::cfg::Sections(std::move(parsed));
                return (PyObject*)res.release();
            } else {
                // Else iterator or TypeError
                auto reader = linereader_from_python(arg_input);
                auto parsed = arki::core::cfg::Sections::parse(*reader, "python object");
                py_unique_ptr<arkipy_cfgSections> res(throw_ifnull(PyObject_New(arkipy_cfgSections, arkipy_cfgSections_Type)));
                new (&(res->sections)) arki::core::cfg::Sections(std::move(parsed));
                return (PyObject*)res.release();
            }
        } ARKI_CATCH_RETURN_PYO
    }
};

struct parse_section : public ClassMethKwargs<parse_section>
{
    constexpr static const char* name = "parse";
    constexpr static const char* signature = "Union[Str, TextIO]";
    constexpr static const char* returns = "arki.cfg.Section";
    constexpr static const char* summary = "parse the named file or open file, and return the resulting Section object";
    constexpr static const char* doc = nullptr;

    static PyObject* run(PyTypeObject* cls, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "input", nullptr };
        PyObject* arg_input = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", const_cast<char**>(kwlist), &arg_input))
            return nullptr;

        try {
            if (PyUnicode_Check(arg_input))
            {
                std::string fname = from_python<std::string>(arg_input);
                arki::utils::sys::File in(fname, O_RDONLY);
                auto parsed = arki::core::cfg::Section::parse(in);
                // If string, open file and parse it
                py_unique_ptr<arkipy_cfgSection> res(throw_ifnull(PyObject_New(arkipy_cfgSection, arkipy_cfgSection_Type)));
                res->owner = nullptr;
                res->section = new arki::core::cfg::Section(std::move(parsed));
                return (PyObject*)res.release();
            } else {
                // Else iterator or TypeError
                auto reader = linereader_from_python(arg_input);
                auto parsed = arki::core::cfg::Section::parse(*reader, "python object");
                py_unique_ptr<arkipy_cfgSection> res(throw_ifnull(PyObject_New(arkipy_cfgSection, arkipy_cfgSection_Type)));
                res->owner = nullptr;
                res->section = new arki::core::cfg::Section(std::move(parsed));
                return (PyObject*)res.release();
            }
        } ARKI_CATCH_RETURN_PYO
    }
};

struct write_sections : public MethKwargs<write_sections, arkipy_cfgSections>
{
    constexpr static const char* name = "write";
    constexpr static const char* signature = "TextIO";
    constexpr static const char* returns = "";
    constexpr static const char* summary = "write the configuration to any object with a write method";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "file", nullptr };
        PyObject* arg_file = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", const_cast<char**>(kwlist), &arg_file))
            return nullptr;

        try {
            TextOutputFile out(arg_file);
            if (out.fd)
                self->sections.write(*out.fd);
            else
                self->sections.write(*out.abstract);
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct write_section : public MethKwargs<write_section, arkipy_cfgSection>
{
    constexpr static const char* name = "write";
    constexpr static const char* signature = "TextIO";
    constexpr static const char* returns = "";
    constexpr static const char* summary = "write the configuration to any object with a write method";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "file", nullptr };
        PyObject* arg_file = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", const_cast<char**>(kwlist), &arg_file))
            return nullptr;

        try {
            TextOutputFile out(arg_file);
            if (out.fd)
                self->section->write(*out.fd);
            else
                self->section->write(*out.abstract);
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};


struct SectionsDef : public Type<SectionsDef, arkipy_cfgSections>
{
    constexpr static const char* name = "Sections";
    constexpr static const char* qual_name = "arkimet.cfg.Sections";
    constexpr static const char* doc = R"(
Arkimet configuration, as multiple sections of key/value options
)";
    GetSetters<> getsetters;
    Methods<section, obtain, sections_get, sections_keys, sections_items, parse_sections, write_sections> methods;

    static void _dealloc(Impl* self)
    {
        self->sections.~Sections();
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

    static int sq_contains(Impl *self, PyObject *value)
    {
        try {
            std::string key = from_python<std::string>(value);
            return (self->sections.find(key) != self->sections.end()) ? 1 : 0;
        } ARKI_CATCH_RETURN_INT
    }

    static PyObject* _iter(Impl* self)
    {
        py_unique_ptr<PyTupleObject> res((PyTupleObject*)PyTuple_New(self->sections.size()));
        unsigned pos = 0;
        for (const auto& si: self->sections)
            PyTuple_SET_ITEM(res, pos++, to_python(si.first));
        return PyObject_GetIter((PyObject*)res.get());
    }

    static Py_ssize_t mp_length(Impl* self)
    {
        return self->sections.size();
    }

    static PyObject* mp_subscript(Impl* self, PyObject* key)
    {
        try {
            std::string name = from_python<std::string>(key);
            arki::core::cfg::Section* res = self->sections.section(name);
            if (res)
                return cfg_section_reference((PyObject*)self, res);
            return PyErr_Format(PyExc_KeyError, "section not found: '%s'", name.c_str());
        } ARKI_CATCH_RETURN_PYO
    }

    static int mp_ass_subscript(Impl* self, PyObject *key, PyObject *val)
    {
        try {
            auto name = from_python<std::string>(key);
            if (!val)
            {
                auto i = self->sections.find(name);
                if (i == self->sections.end())
                {
                    PyErr_Format(PyExc_KeyError, "section not found: '%s'", name.c_str());
                    return -1;
                }
                self->sections.erase(i);
            } else {
                std::string k = from_python<std::string>(key);
                auto i = self->sections.find(k);
                if (i == self->sections.end())
                    self->sections.emplace(k, section_from_python(val));
                else
                    i->second = section_from_python(val);
            }
            return 0;
        } ARKI_CATCH_RETURN_INT
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { nullptr };
        if (!PyArg_ParseTupleAndKeywords(args, kw, "", const_cast<char**>(kwlist)))
            return -1;

        try {
            new(&(self->sections)) arki::core::cfg::Sections();
        } ARKI_CATCH_RETURN_INT

        return 0;
    }
};

SectionsDef* sections_def = nullptr;


void fill_section_from_dict(arki::core::cfg::Section& dest, PyObject* src)
{
    PyObject *key, *value;
    Py_ssize_t pos = 0;

    while (PyDict_Next(src, &pos, &key, &value)) {
        dest.set(from_python<std::string>(key), from_python<std::string>(value));
    }
}


struct SectionDef : public Type<SectionDef, arkipy_cfgSection>
{
    constexpr static const char* name = "Section";
    constexpr static const char* qual_name = "arkimet.cfg.Section";
    constexpr static const char* doc = R"(
Arkimet configuration, as a section of key/value options
)";
    GetSetters<> getsetters;
    Methods<section_keys, section_get, section_items, parse_section, write_section> methods;

    static void _dealloc(Impl* self)
    {
        if (self->owner)
            Py_DECREF(self->owner);
        else
            delete self->section;
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

    static int sq_contains(Impl *self, PyObject *value)
    {
        try {
            std::string key = from_python<std::string>(value);
            return self->section->has(key) ? 1 : 0;
        } ARKI_CATCH_RETURN_INT
    }

    static PyObject* _iter(Impl* self)
    {
        py_unique_ptr<PyTupleObject> res((PyTupleObject*)PyTuple_New(self->section->size()));
        unsigned pos = 0;
        for (const auto& si: *self->section)
            PyTuple_SET_ITEM(res, pos++, to_python(si.first));
        return PyObject_GetIter((PyObject*)res.get());
    }

    static Py_ssize_t mp_length(Impl* self)
    {
        return self->section->size();
    }

    static PyObject* mp_subscript(Impl* self, PyObject* key)
    {
        try {
            std::string k = from_python<std::string>(key);
            if (!self->section->has(k))
                return PyErr_Format(PyExc_KeyError, "section not found: '%s'", k.c_str());
            else
                return to_python(self->section->value(k));
        } ARKI_CATCH_RETURN_PYO
    }

    static int mp_ass_subscript(Impl* self, PyObject *key, PyObject *val)
    {
        try {
            std::string k = from_python<std::string>(key);
            if (!val)
            {
                auto i = self->section->find(name);
                if (i == self->section->end())
                {
                    PyErr_Format(PyExc_KeyError, "key not found: '%s'", k.c_str());
                    return -1;
                }
                self->section->erase(i);
            } else
                self->section->set(k, from_python<std::string>(val));
            return 0;
        } ARKI_CATCH_RETURN_INT
    }

    static PyObject* _richcompare(Impl* self, PyObject *other, int op)
    {
        try {
            switch (op)
            {
                case Py_EQ:
                    if (arkipy_cfgSection_Check(other))
                    {
                        if (self->section == ((Impl*)other)->section)
                            Py_RETURN_TRUE;
                        else
                            Py_RETURN_FALSE;
                    } else
                        return Py_NotImplemented;
                default:
                    return Py_NotImplemented;
            }
        } ARKI_CATCH_RETURN_PYO
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        PyObject* init_dict = nullptr;

        if (PyTuple_GET_SIZE(args) == 1)
        {
            PyObject* first = PyTuple_GET_ITEM(args, 0);
            if (!PyDict_Check(first))
            {
                PyErr_SetString(PyExc_TypeError, "if a positional argument is provided to arkimet.cfg.Section(), it must be a dict");
                return -1;
            }

            init_dict = first;
        } else if (kw && PyDict_Size(kw) > 0) {
            init_dict = kw;
        }

        try {
            int argc = PyTuple_GET_SIZE(args);
            if (argc > 1)
            {
                PyErr_SetString(PyExc_TypeError, "arkimet.cfg.Section() takes at most one positional argument");
                return -1;
            }

            self->owner = nullptr;
            self->section = new arki::core::cfg::Section;

            if (init_dict)
                fill_section_from_dict(*(self->section), init_dict);
        } ARKI_CATCH_RETURN_INT

        return 0;
    }
};

SectionDef* section_def = nullptr;


Methods<> cfg_methods;

}



extern "C" {

static PyModuleDef cfg_module = {
    PyModuleDef_HEAD_INIT,
    "cfg",          /* m_name */
    "Arkimet configuration C functions",  /* m_doc */
    -1,             /* m_size */
    cfg_methods.as_py(),    /* m_methods */
    nullptr,           /* m_slots */
    nullptr,           /* m_traverse */
    nullptr,           /* m_clear */
    nullptr,           /* m_free */
};

}

namespace arki {
namespace python {

// TODO: Sections as_configparser
// TODO: Section as_dict
// TODO: Sections constructor with configparser
// TODO: Section constructor with dict
// TODO: Section constructor with sequence of (key, value)

core::cfg::Section section_from_python(PyObject* o)
{
    try {
        if (arkipy_cfgSection_Check(o)) {
            return *((arkipy_cfgSection*)o)->section;
        }

        if (PyBytes_Check(o)) {
            const char* v = throw_ifnull(PyBytes_AsString(o));
            return core::cfg::Section::parse(v);
        }
        if (PyUnicode_Check(o)) {
            const char* v = throw_ifnull(PyUnicode_AsUTF8(o));
            return core::cfg::Section::parse(v);
        }
        if (PyDict_Check(o))
        {
            core::cfg::Section res;
            PyObject *key, *val;
            Py_ssize_t pos = 0;
            while (PyDict_Next(o, &pos, &key, &val))
                res.set(string_from_python(key), string_from_python(val));
            return res;
        }
        PyErr_SetString(PyExc_TypeError, "value must be an instance of str, bytes, or dict");
        throw PythonException();
    } ARKI_CATCH_RETHROW_PYTHON
}

core::cfg::Sections sections_from_python(PyObject* o)
{
    try {
        if (arkipy_cfgSections_Check(o)) {
            return ((arkipy_cfgSections*)o)->sections;
        }

        if (PyBytes_Check(o)) {
            const char* v = throw_ifnull(PyBytes_AsString(o));
            return core::cfg::Sections::parse(v);
        }
        if (PyUnicode_Check(o)) {
            const char* v = throw_ifnull(PyUnicode_AsUTF8(o));
            return core::cfg::Sections::parse(v);
        }
        PyErr_SetString(PyExc_TypeError, "value must be an instance of str, or bytes");
        throw PythonException();
    } ARKI_CATCH_RETHROW_PYTHON
}


PyObject* cfg_sections(const core::cfg::Sections& sections)
{
    py_unique_ptr<arkipy_cfgSections> res(throw_ifnull(PyObject_New(arkipy_cfgSections, arkipy_cfgSections_Type)));
    new (&(res->sections)) core::cfg::Sections(sections);
    return (PyObject*)res.release();
}

PyObject* cfg_sections(core::cfg::Sections&& sections)
{
    py_unique_ptr<arkipy_cfgSections> res(throw_ifnull(PyObject_New(arkipy_cfgSections, arkipy_cfgSections_Type)));
    new (&(res->sections)) core::cfg::Sections(std::move(sections));
    return (PyObject*)res.release();
}

PyObject* cfg_section(const core::cfg::Section& section)
{
    py_unique_ptr<arkipy_cfgSection> res(throw_ifnull(PyObject_New(arkipy_cfgSection, arkipy_cfgSection_Type)));
    res->owner = nullptr;
    res->section = new core::cfg::Section(section);
    return (PyObject*)res.release();
}

PyObject* cfg_section(core::cfg::Section&& section)
{
    py_unique_ptr<arkipy_cfgSection> res(throw_ifnull(PyObject_New(arkipy_cfgSection, arkipy_cfgSection_Type)));
    res->owner = nullptr;
    res->section = new core::cfg::Section(std::move(section));
    return (PyObject*)res.release();
}

PyObject* cfg_section_reference(PyObject* owner, core::cfg::Section* section)
{
    py_unique_ptr<arkipy_cfgSection> res(throw_ifnull(PyObject_New(arkipy_cfgSection, arkipy_cfgSection_Type)));
    res->owner = owner;
    res->section = section;
    Py_INCREF(owner);
    return (PyObject*)res.release();
}

void register_cfg(PyObject* m)
{
    pyo_unique_ptr cfg = throw_ifnull(PyModule_Create(&cfg_module));

    sections_def = new SectionsDef;
    sections_def->define(arkipy_cfgSections_Type, cfg);

    section_def = new SectionDef;
    section_def->define(arkipy_cfgSection_Type, cfg);

    if (PyModule_AddObject(m, "cfg", cfg.release()) == -1)
        throw PythonException();
}

}
}

