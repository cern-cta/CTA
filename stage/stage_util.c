/*
 * $Id: stage_util.c,v 1.2 2001/02/05 20:56:11 jdurand Exp $
 */

#include <sys/types.h>
#ifndef _WIN32
#include <sys/time.h>
#include <unistd.h>
#else
#include <winsock2.h>
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
