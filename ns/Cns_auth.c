/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA Ben Couturier
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "$id$";
#endif /* not lint */

/*	Cns_auth.c - Sets the authorization in the CASTOR name server API */

#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include "Cns_api.h"
#include "serrno.h"
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <unistd.h>
#endif

#define MODE_EFFECTIVE_ID 0
#define MODE_REAL_ID      1

/** Cns_setid
 *
 * Sets the authorization id in the per thread structure.
 * In any case, in authenticated mode, these uid/gid are tructed only
 * if the client runs with service credentials.
 *
 * @param uid The desired uid
 * @param gid The desired gid
 * @returns 0 in case of successs, -1 otherwise.
 */
int DLL_DECL
Cns_setid(uid, gid)
 uid_t uid;
 gid_t gid;
{
  char func[16];
  struct Cns_api_thread_info *thip;
  
  strcpy (func, "Cns_setid");
  if (Cns_apiinit (&thip))
    return (-1);
  thip->uid = uid;
  thip->gid = gid;
  thip->use_authorization_id = 1;
  return(0);
}

/** Cns_unsetid
 *
 * Resets the authorization id in the per thread structure.
 * @returns 0 in case of successs, -1 otherwise.
 */
int DLL_DECL
Cns_unsetid()
{
 
  char func[16];
  struct Cns_api_thread_info *thip;
   
  strcpy (func, "Cns_unsetid");
  if (Cns_apiinit (&thip))
    return (-1);
  thip->uid = 0;
  thip->gid = 0;
  thip->use_authorization_id = 0;
  return(0);
}


/** Cns_getid_ext
 *
 * Gets the authorization id from the per thread structure.
 * Either Cns_setid has been done, in which case that id is used,
 * or getuid/getgid are used to get the values.
 *
 * @param uid Pointer to where to store the uid
 * @param gid Pointer to where to store the gid
 * @param mode Indicates wheteher real of effective uid 
 *             should be looked up by default
 * @returns 0 in case of successs, -1 otherwise.
 */
static int DLL_DECL
Cns_getid_ext(uid, gid, mode)
 uid_t *uid;
 gid_t *gid;
 int mode;
{
  char func[16];
  struct Cns_api_thread_info *thip;
  
  strcpy (func, "Cns_getid");
  if (Cns_apiinit (&thip))
    return (-1);
  
  if (uid != NULL) {
    if(thip->use_authorization_id) {
      *uid = thip->uid;
    } else {
      if (mode == MODE_REAL_ID) {
	*uid = getuid();
      } else {
	*uid = geteuid();
      }
    }
  }

  if (gid != NULL) {
    if(thip->use_authorization_id) {
      *gid = thip->gid;
    } else {
      if (mode == MODE_REAL_ID) {
	*gid = getgid();
      } else {
	*gid = getegid();
      }
    }
  }
  return(0);
}


/** Cns_getid
 *
 * Gets the authorization id from the per thread structure.
 * Either Cns_setid has been done, in which case that id is used,
 * or geteuid/getegid are used to get the values.
 *
 * @param uid Pointer to where to store the uid
 * @param gid Pointer to where to store the gid
 * @returns 0 in case of successs, -1 otherwise.
 */
int DLL_DECL
Cns_getid(uid, gid)
 uid_t *uid;
 gid_t *gid;
{
  return Cns_getid_ext(uid, gid, MODE_EFFECTIVE_ID);
}

/** Cns_getrealid
 *
 * Gets the authorization id from the per thread structure.
 * Either Cns_setid has been done, in which case that id is used,
 * or geteuid/getegid are used to get the values.
 *
 * @param uid Pointer to where to store the uid
 * @param gid Pointer to where to store the gid
 * @returns 0 in case of successs, -1 otherwise.
 */
int DLL_DECL
Cns_getrealid(uid, gid)
 uid_t *uid;
 gid_t *gid;
{
  return Cns_getid_ext(uid, gid, MODE_REAL_ID);
}
