/*
 * $Id: stage_util.c,v 1.1 2001/01/31 19:03:46 jdurand Exp $
 */

#include <sys/time.h>
#include <sys/types.h>
#ifndef _WIN32
#include <unistd.h>
#endif
#include "stage_api.h"
#include "osdep.h"

void DLL_DECL stage_sleep(nsec)
     int nsec;
{
  struct timeval ts;

  if (nsec <= 0) return;

  ts.tv_sec = nsec;
  ts.tv_usec = 0;
  select(0,NULL,NULL,NULL,&ts);
}
