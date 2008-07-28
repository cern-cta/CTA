/*
 * 
 * Copyright (C) 2004 by CERN/IT/ADC
 * All rights reserved
 *
 */

/*	expexec - execute external program */
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#include <unistd.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include "serrno.h"
#include "expert_api.h"

#define EXP_STRBUFLEN 1024
int main(argc, argv)
int argc;
char **argv;
{

        int socket;
	int n;
	char buffer[4096];

#if defined(_WIN32)
	WSADATA wsadata;

	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, EXP52);
		exit (EXP_SYERR);
	}
#endif
	n = expert_send_request(&socket, EXP_EXECUTE);
	if (n < 0) {
	  fprintf (stdout, "Error sending request: serrno = %d %s\n", serrno, sstrerror(serrno));
	  exit (serrno);
	}
	while ((n = expert_receive_data(socket, buffer, sizeof(buffer), 10)) > 0) {
       	  write(1, buffer, n);
	}

	exit (0);
}
