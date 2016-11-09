#include <Python.h>
#include "common.h"
#include "metadata.h"
#include "summary.h"
#include "dataset.h"
#include "arki/matcher.h"
#include "arki/configfile.h"
#include "arki/runtime.h"
#include "arki/runtime/arkiquery.h"
#include "config.h"

using namespace std;
using namespace arki;
using namespace arki::python;

extern "C" {

static PyObject* arkipy_expand_query(PyTypeObject *type, PyObject *args)
{
    const char* query;
    if (!PyArg_ParseTuple(args, "s", &query))
        return nullptr;
    try {
        Matcher m = Matcher::parse(query);
        string out = m.toStringExpanded();
        return PyUnicode_FromStringAndSize(out.data(), out.size());
    } ARKI_CATCH_RETURN_PYO
}

static PyObject* arkipy_matcher_alias_database(PyTypeObject *type, PyObject *none)
{
    try {
        ConfigFile cfg;
        MatcherAliasDatabase::serialise(cfg);
        string out = cfg.serialize();
        return PyUnicode_FromStringAndSize(out.data(), out.size());
    } ARKI_CATCH_RETURN_PYO
}

static PyObject* arkipy_make_qmacro_dataset(arkipy_Metadata* self, PyObject *args, PyObject* kw)
{
    static const char* kwlist[] = { "cfg", "datasets", "name", "query", NULL };
    PyObject* arg_cfg = Py_None;
    PyObject* arg_datasets = Py_None;
    const char* name = nullptr;
    const char* query = "";

    if (!PyArg_ParseTupleAndKeywords(args, kw, "OOs|s", (char**)kwlist, &arg_cfg, &arg_datasets, &name, &query))
        return nullptr;

    ConfigFile cfg;
    if (configfile_from_python(arg_cfg, cfg)) return nullptr;
    ConfigFile datasets;
    if (configfile_from_python(arg_datasets, datasets)) return nullptr;

    try {
        std::unique_ptr<dataset::Reader> ds = runtime::make_qmacro_dataset(cfg, datasets, name, query);
        return (PyObject*)dataset_reader_create(move(ds));
    } ARKI_CATCH_RETURN_PYO
}

struct EnvOverride
{
    bool was_set;
    std::string key;
    std::string orig;

    EnvOverride(const char* key, const char* val) : key(key)
    {
        const char* o = getenv(key);
        was_set = o != nullptr;
        if (was_set) orig = o;

        if (val)
            setenv(key, val, 1);
        else
            unsetenv(key);
    }

    ~EnvOverride()
    {
        if (was_set)
            setenv(key.c_str(), orig.c_str(), 1);
        else
            unsetenv(key.c_str());
    }
};

static PyObject* arkipy_arki_query(arkipy_Metadata* self, PyObject *args, PyObject* kw)
{
    static const char* kwlist[] = {
        "query", "input", "summary", "summary_short",
        "yaml", "json", "annotate", "data_only", "data_inline",
        "postprocess", "report", "summary_restrict", "sort", "targetfile",
        "output", "postproc_data", "qmacro", "merged",
        NULL };
    const char* query = "";
    PyObject* input = Py_None;
    const char* postprocess = nullptr;
    const char* report = nullptr;
    const char* summary_restrict = nullptr;
    const char* sort = nullptr;
    const char* targetfile = nullptr;
    const char* outfile = "";
    const char* postproc_data = nullptr;
    const char* qmacro = nullptr;
    int summary = 0;
    int summary_short = 0;
    int yaml = 0;
    int json = 0;
    int annotate = 0;
    int data_only = 0;
    int data_inline = 0;
    int merged = 0;

    unique_ptr<runtime::ArkiQuery> tool(new runtime::ArkiQuery);

    if (!PyArg_ParseTupleAndKeywords(args, kw,
                "|sOpp"
                "ppppp"
                "sssss"
                "sssp",
                (char**)kwlist,
                &query, &input, &summary, &summary_short,
                &yaml, &json, &annotate, &data_only, &data_inline,
                &postprocess, &report, &summary_restrict, &sort, &targetfile,
                &outfile, &postproc_data, &qmacro, &merged
            ))
        return nullptr;

    auto& pmaker = tool->pmaker;
    pmaker.summary = summary;
    pmaker.summary_short = summary_short;
    pmaker.yaml = yaml;
    pmaker.json = json;
    pmaker.annotate = annotate;
    pmaker.data_only = data_only;
    pmaker.data_inline = data_inline;
    if (postprocess) pmaker.postprocess = postprocess;
    if (report) pmaker.report = report;
    if (summary_restrict) pmaker.summary_restrict = summary_restrict;
    if (sort) pmaker.sort = sort;
    if (targetfile) pmaker.targetfile = targetfile;
    tool->merged = merged;

    // Run here a consistency check on the processor maker configuration
    std::string errors = tool->pmaker.verify_option_consistency();
    if (!errors.empty())
    {
        PyErr_SetString(PyExc_RuntimeError, errors.c_str());
        return nullptr;
    }

    if (configfile_from_python(input, tool->input_info)) return nullptr;
    tool->set_output(outfile);

    if (qmacro) tool->qmacro = qmacro;

    tool->set_query(query);

    EnvOverride postproc_env("ARKI_POSTPROC_FILES", postproc_data);
    try {
        tool->main();
        Py_RETURN_NONE;
    } ARKI_CATCH_RETURN_PYO
}

static PyMethodDef arkimet_methods[] = {
    { "expand_query", (PyCFunction)arkipy_expand_query, METH_VARARGS, "Return the same text query with all aliases expanded" },
    { "matcher_alias_database", (PyCFunction)arkipy_matcher_alias_database, METH_NOARGS, "Return a string with the current matcher alias database" },
    { "make_qmacro_dataset", (PyCFunction)arkipy_make_qmacro_dataset, METH_VARARGS, R"(
        Create a qmacro dataset that aggregates the contents of multiple datasets.

        Arguments:
          cfg: base configuration of the resulting dataset
          datasets: configuration of all the available datasets
          name: name of the query macro to use
        )" },
    { "arki_query", (PyCFunction)arkipy_arki_query, METH_VARARGS | METH_KEYWORDS, R"(
        Run the arki-query engine.

        Arguments:
          query: matcher query
          input: configuration for all input sources
          output: output file name, or "-" for standard output
          postproc_data: colon-separated list of file names to pass to the postprocessor

        These boolean arguments are equivalent to the arki-query options with
        the same name:
          summary
          summary_short
          yaml
          json
          annotate
          data_only
          data_inline
          merged

        These string arguments are equivalent to the arki-query options with
        the same name:
          postprocess
          report
          summary_restrict
          sort
          targetfile
          qmacro
        )" },
    { NULL }
};

static PyModuleDef arkimet_module = {
    PyModuleDef_HEAD_INIT,
    "_arkimet",       /* m_name */
    "Arkimet Python interface.",  /* m_doc */
    -1,             /* m_size */
    arkimet_methods, /* m_methods */
    NULL,           /* m_reload */
    NULL,           /* m_traverse */
    NULL,           /* m_clear */
    NULL,           /* m_free */

};

PyMODINIT_FUNC PyInit__arkimet(void)
{
    using namespace arki::python;

    PyObject* m = PyModule_Create(&arkimet_module);

    register_metadata(m);
    register_summary(m);
    register_dataset(m);

    return m;
}

}
