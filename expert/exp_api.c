/*
*
* Copyright (C) 2004 by CERN/IT/ADC
* All rights reserved
*
*/


/*********************************************************************

  API interface to the CASTOR Expert Facility
  
 ----------------------------------------------------------------------
    
 int expert_send_request (int *socket, int request)
	  
 Sends request to the expert server.
 Parameters: int *socket - pointer to the integer the socket descriptor
             will be placed in. You need this descriptor for subsequent
             calls to expert_send_data() and expert_receive_data()

             int request - specifies the request which should be processed and
             must be one of EXP_RQ_FILESYSTEM, EXP_RQ_MIGRATOR, EXP_RQ_RECALLER, EXP_RQ_GC, ...
 Return value: 0 on success (request accepted), < 0 on error - 
               serrno should be set accordingly.

 ----------------------------------------------------------------------

 int expert_send_data(int socket, const char *buffer, int buf_length)

 Sends data to the expert system.
 Parameters: int socket - the socket descriptor, which has been initialized
             by the call to expert_send_request()
             const char *buffer - pointer to the data buffer.
             int buf_length - buffer length.
 Return value: number of sent bytes on success, -1 on error.

 ----------------------------------------------------------------------
 
 int expert_receive_data(int socket, char *buffer, int buf_length, int timeout)

 Receives data from the expert system.
 This function tries to fill the buffer and returns when the buffer is full,
 connection closed by the server, or timeout or error occures.
 Parameters: int socket - the socket descriptor, which has been initialized
             by the call to expert_send_request().
             char *buffer - pointer to the buffer data should be placed in.
             int buf_length - buffer length.
             int timeout - specifies the read timeout in seconds.
 Return value: number of received bytes if data were received independent of
               the completion status of the last recv(), or <= 0 on error.
			  
***********************************************************************/
#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include "net.h"
#include "marshall.h"
#include "serrno.h"
#include "expert.h"

int DLL_DECL expert_send_request(exp_socket, request)
int *exp_socket;
int request;
{

	gid_t gid;
	int msglen;
	char *q;
	char *sbp;
	char sendbuf[EXP_REQBUFSZ];
	uid_t uid;

	int status;
	int rep_type;
	int errcode;

	uid = geteuid();
	gid = getegid();

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, EXP_MAGIC);
	marshall_LONG (sbp, request);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);      /* update length field */

	*exp_socket = -1;

	if (send2expert (exp_socket, sendbuf, msglen) < 0)
	  return (-1);

	/* Get reply status */

	if (getexpertrep (*exp_socket, &status, &errcode, &rep_type) < 0)
	  return (-1); /* Error getting reply */

	/************************************************************************************/
	if (rep_type != EXP_RP_STATUS) {
	  serrno = SEINTERNAL;
	  return (-1);
	}
	
	/* Check status */

	if (status == EXP_ST_ACCEPTED)
	  return (0);
	else {
	  serrno = errcode;
	  return (-1);
	}
}

int DLL_DECL expert_send_data(exp_socket, buffer, buf_length)
int exp_socket;
const char *buffer;
int buf_length;
{
        int n;
        if ((n = netwrite (exp_socket, (char *)buffer, buf_length)) <= 0) {
		(void) netclose (exp_socket);
		serrno = SECOMERR;
		return (-1);
	}
	return (n);
}


int DLL_DECL expert_netread_timeout(exp_socket, buffer, buf_length, timeout)
int exp_socket;
char *buffer;
int buf_length;
int timeout;
{
    fd_set  fds;
    struct  timeval tout;

    FD_ZERO (&fds);
    FD_SET  (exp_socket, &fds);
    tout.tv_sec = timeout;
    tout.tv_usec = 0;

    switch(select(FD_SETSIZE, &fds, (fd_set *)0, (fd_set *)0, &tout)) {
    case -1:
        return (-1);
    case 0:
        serrno = SETIMEDOUT;
        return(-1);
    default:
        break;
    }
    return (recv(exp_socket, buffer, buf_length, 0));
}



int DLL_DECL expert_receive_data(exp_socket, buffer, buf_length, timeout)
int exp_socket;
char *buffer;
int buf_length;
int timeout;
{
        char *p;
	int l;
	int c;
	int n;
	int el;

	serrno = 0; /* Clear error */
	el = strlen(EXP_ERRSTRING);
	if (buf_length < el || timeout <= 0) {
	  serrno = EINVAL;
	  return (-1);
	}

        for (p = buffer, l = buf_length, c = 0; l > 0;) {
	  n = expert_netread_timeout(exp_socket, p, l, timeout);
	  if (n <= 0) break;
	  c += n;
	  p += n;
	  l -= n;
	}
        if (n <= 0) netclose (exp_socket);
	if (c > 0) { /* Data have been received */
	  if (c >= el) {
	    if (strncmp(buffer, EXP_ERRSTRING, el) == 0) {
	      serrno = EEXPEXECV;
	      return (-1);
	    }
	  }
	  return (c);
	}
	else
	  return (n);
}

