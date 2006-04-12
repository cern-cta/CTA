#ifndef _POLL_H_H_
#define _POLL_H_H_
/*
**  Emulate poll() for those platforms (e.g., Ultrix) that don't have it.
**  Usually select() and poll() are the same functionally, differing only
**  in the calling interface, but in a few cases they behave differently,
**  possibly where the platform implements STREAMS devices and one or the
**  other has some compatibility bug.  Poll() has a cleaner programming
**  interface, anyway.
*/

#ifdef HAVE_POLL

# include <sys/poll.h>

#else

#include "osdep.h"

struct pollfd {
	int fd;
	short events;
	short revents;
};

#define POLLIN 001
#define POLLPRI 002
#define POLLOUT 004
#define POLLNORM POLLIN
#define POLLERR 010
#define POLLHUP 020
#define POLLNVAL 040

EXTERN_C int DLL_DECL poll _PROTO((struct pollfd *, unsigned int, int));

#endif
#endif /* _POLL_H_H_ */
