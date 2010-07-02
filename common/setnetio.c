/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* setnetio.c   Set network input/output characteristics                */

#include <trace.h>                      /* tracing definitions          */

extern int (*recvfunc)();
extern int (*sendfunc)();

extern int s_recv();            /*         Normal recv()                */
extern int s_send();            /*         Normal send()                */

int setnetio() {
	recvfunc = s_recv;
	sendfunc = s_send;
	END_TRACE();
	return(0);
}
