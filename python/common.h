#ifndef ARKI_PYTHON_COMMON_H
#define ARKI_PYTHON_COMMON_H

#include <Python.h>
#include <string>
#include <vector>
#include <arki/emitter.h>

namespace arki {
struct ConfigFile;

namespace python {

struct python_callback_failed : public std::exception {};


/// Given a generic exception, set the Python error indicator appropriately.
void set_std_exception(const std::exception& e);


#define ARKI_CATCH_RETURN_PYO \
      catch (python_callback_failed&) { \
        return nullptr; \
    } catch (std::exception& se) { \
        set_std_exception(se); return nullptr; \
    }

#define ARKI_CATCH_RETURN_INT \
      catch (python_callback_failed&) { \
        return -1; \
    } catch (std::exception& se) { \
        set_std_exception(se); return -1; \
    }

/**
 * unique_ptr-like object that contains PyObject pointers, and that calls
 * Py_DECREF on destruction.
 */
template<typename Obj>
class py_unique_ptr
{
protected:
    Obj* ptr;

public:
    py_unique_ptr() : ptr(nullptr) {}
    py_unique_ptr(Obj* o) : ptr(o) {}
    py_unique_ptr(const py_unique_ptr&) = delete;
    py_unique_ptr(py_unique_ptr&& o) : ptr(o.ptr) { o.ptr = nullptr; }
    ~py_unique_ptr() { Py_XDECREF(ptr); }
    py_unique_ptr& operator=(const py_unique_ptr&) = delete;
    py_unique_ptr& operator=(py_unique_ptr&& o)
    {
        if (this == &o) return *this;
        Py_XDECREF(ptr);
        ptr = o.ptr;
        o.ptr = nullptr;
        return *this;
    }

    /// Release the reference without calling Py_DECREF
    Obj* release()
    {
        Obj* res = ptr;
        ptr = nullptr;
        return res;
    }

    /// Use it as a Obj
    operator Obj*() { return ptr; }

    /// Use it as a Obj
    Obj* operator->() { return ptr; }

    /// Get the pointer (useful for passing to Py_BuildValue)
    Obj* get() { return ptr; }

    /// Check if ptr is not nullptr
    operator bool() const { return ptr; }
};

typedef py_unique_ptr<PyObject> pyo_unique_ptr;

struct PythonEmitter : public Emitter
{
    struct Target
    {
        enum State {
            LIST,
            MAPPING,
            MAPPING_KEY,
        } state;
        PyObject* o = nullptr;

        Target(State state, PyObject* o) : state(state), o(o) {}
    };
    std::vector<Target> stack;
    PyObject* res = nullptr;

    ~PythonEmitter();

    PyObject* release()
    {
        PyObject* o = res;
        res = nullptr;
        return o;
    }

    /**
     * Adds a value to the python object that is currnetly been built.
     *
     * Steals a reference to o.
     */
    void add_object(PyObject* o);

    void start_list() override;
    void end_list() override;

    void start_mapping() override;
    void end_mapping() override;

    void add_null() override;
    void add_bool(bool val) override;
    void add_int(long long int val) override;
    void add_double(double val) override;
    void add_string(const std::string& val) override;
};

#if 0
extern wrpy_c_api* wrpy;

/// Convert a Datetime to a python datetime object
PyObject* datetime_to_python(const Datetime& dt);

/// Convert a python datetime object to a Datetime
int datetime_from_python(PyObject* dt, Datetime& out);

/// Convert a sequence of two python datetime objects to a DatetimeRange
int datetimerange_from_python(PyObject* dt, DatetimeRange& out);

/// Convert a Level to a python 4-tuple
PyObject* level_to_python(const Level& lev);

/// Convert a 4-tuple to a Level
int level_from_python(PyObject* o, Level& out);

/// Convert a Trange to a python 3-tuple
PyObject* trange_to_python(const Trange& tr);

/// Convert a 3-tuple to a Trange
int trange_from_python(PyObject* o, Trange& out);

/**
 * call o.data() and return its result, both as a PyObject and as a buffer.
 *
 * The data returned in buf and len will be valid as long as the returned
 * object stays valid.
 */
PyObject* file_get_data(PyObject* o, char*&buf, Py_ssize_t& len);
#endif

/// Call repr() on \a o, and return the result in \a out
int object_repr(PyObject* o, std::string& out);

/**
 * If o is a Long, return its value. Else call o.fileno() and return its
 * result.
 *
 * Returns -1 if fileno() was not available or an error occurred.
 */
int file_get_fileno(PyObject* o);

/**
 * Convert a python string, bytes or unicode to an utf8 string.
 *
 * Return 0 on success, -1 on failure
 */
int string_from_python(PyObject* o, std::string& out);

/**
 * Fill a ConfigFile with configuration info from python.
 *
 * Currently this only supports:
 *  - str or bytes, that will get parsed by ConfigFile.
 *  - dict, that will be set as key->val into out
 *
 * In the future it can support reading data from a ConfigFile object.
 */
int configfile_from_python(PyObject* o, ConfigFile& out);

/**
 * Initialize the python bits to use used by the common functions.
 *
 * This can be called multiple times and will execute only once.
 */
int common_init();

}
}
#endif