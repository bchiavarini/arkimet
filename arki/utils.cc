#include "config.h"
#define _XOPEN_SOURCE 700
#define _XOPEN_SOURCE_EXTENDED 1
#include "arki/utils.h"
#include "arki/exceptions.h"
#include "arki/utils/sys.h"
#include "arki/wibble/sys/process.h"
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

using namespace std;

#ifndef HAVE_MKDTEMP
/* Replacement mkdtemp if not provided by libc */
char *mkdtemp(char *dir_template) {
  if (dir_template == NULL) { errno=EINVAL; return NULL; };
  if (strlen(dir_template) < 6 ) { errno=EINVAL; return NULL; };
  if (strcmp(dir_template + strlen(dir_template) - 6, "XXXXXX") != 0)
{ errno=EINVAL; return NULL; };
  char * pos = dir_template + strlen(dir_template) - 6;

  do {
      int num = rand() % 1000000;
      int res = 0;
      sprintf(pos, "%06d", num);
      res = mkdir(dir_template, 0755);
      if (res == 0) { return dir_template; }; // success
      if (errno == EEXIST) { continue; }; // try with a different number
      return NULL; // pass mkdir errorcode otherwise
  } while (0);
  return NULL;
}
#endif

namespace arki {
namespace utils {

void createFlagfile(const std::string& pathname)
{
    sys::File fd(pathname, O_WRONLY | O_CREAT | O_NOCTTY, 0666);
    fd.close();
}

void createNewFlagfile(const std::string& pathname)
{
    sys::File fd(pathname, O_WRONLY | O_CREAT | O_NOCTTY | O_EXCL, 0666);
    fd.close();
}

void hexdump(const char* name, const std::string& str)
{
	fprintf(stderr, "%s ", name);
	for (string::const_iterator i = str.begin(); i != str.end(); ++i)
	{
		fprintf(stderr, "%02x ", (int)(unsigned char)*i);
	}
	fprintf(stderr, "\n");
}

void hexdump(const char* name, const unsigned char* str, int len)
{
	fprintf(stderr, "%s ", name);
	for (int i = 0; i < len; ++i)
	{
		fprintf(stderr, "%02x ", str[i]);
	}
	fprintf(stderr, "\n");
}

}
}
