#include "h/getconfent.h"
#include "h/rtcpdIsConfiguredToReuseMount.h"

#include <ctype.h>
#include <stdlib.h>
#include <string.h>

EXTERN_C int rtcpdIsConfiguredToReuseMount() {
  const char *const origParamValue = getconfent("RTCOPYD", "REUSE_MOUNT", 0);
  char *upperCaseParamValue = 0;
  const int defaultValue = 1;
  int returnValue = 0;

  /* Return the default value if there is no parameter value */
  if(0 == origParamValue) {
    return defaultValue;
  }

  /* Duplicate the parameter value */
  upperCaseParamValue = strdup(origParamValue);

  /* Return the default value if insufficient memory was available */
  if(0 == upperCaseParamValue) {
    return defaultValue;
  }

  /* Convert the parameter value to upper case */
  {
    char *p = upperCaseParamValue;
    while(*p != '\0') {
      *p = toupper(*p);
      p++;
    }
  }

  /* Determine the return value */
  if(0 == strcmp(upperCaseParamValue, "TRUE")) {
    returnValue = 1;
  } else {
    returnValue = 0;
  }

  /* Free the memory used to convert the parameter value to upper case */
  free(upperCaseParamValue);

  return returnValue;
}
