#include "h/getconfent.h"
#include "h/rtcpdIsConfiguredToReuseMount.h"

#include <ctype.h>
#include <stdlib.h>
#include <string.h>

EXTERN_C int rtcpdIsConfiguredToReuseMount() {
  const char *const origParamValue = getconfent("RTCOPYD", "REUSE_MOUNT", 0);
  char *upperCaseParamValue = 0;
  int returnValue = 0;

  /* Return false if there is no parameter value */
  if(0 == origParamValue) {
    return 0;
  }

  /* Duplicate the parameter value */
  upperCaseParamValue = strdup(origParamValue);

  /* Return false if insufficeint memory was available */
  if(0 == upperCaseParamValue) {
    return 0;
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
