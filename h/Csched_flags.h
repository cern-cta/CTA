/*
 * $Id: Csched_flags.h,v 1.1 2000/06/19 09:48:34 jdurand Exp $
 */

#ifndef __Csched_flags_h
#define __Csched_flags_h

#include <Cthread_flags.h>
/* ----------------------------------------------- */
/* Now that Thread Environment is loaded           */
/* (<pthread.h>) we can determine if at run        */
/* time we can support sort thread (very) specific */
/* functionnality.                                 */
/* ----------------------------------------------- */
/* - First In, First Out */
#ifdef CSCHED_FIFO
#undef CSCHED_FIFO
#endif
/* - Round Robin */
#ifdef CSCHED_RR
#undef CSCHED_RR
#endif
/* - Default (other) */
#ifdef CSCHED_OTHER
#undef CSCHED_OTHER
#endif
/* - Default (new primitive or non portable) */
#ifdef CSCHED_FG_NP
#undef CSCHED_FG_NP
#endif
/* - Background */
#ifdef CSCHED_BG_NP
#undef CSCHED_BG_NP
#endif
/* - Unknown */
#ifdef CSCHED_UNKNOWN
#undef CSCHED_UNKNOWN
#endif
#define CSCHED_UNKNOWN -1

#ifdef _CTHREAD
/* Thread Environment */
/* - First In, First Out */
#ifdef SCHED_FIFO
#define CSCHED_FIFO SCHED_FIFO
#else
#define CSCHED_FIFO CSCHED_UNKNOWN
#endif
/* - Round Robin */
#ifdef SCHED_RR
#define CSCHED_RR SCHED_RR
#else
#define CSCHED_RR CSCHED_UNKNOWN
#endif
/* - Default (other) */
#ifdef SCHED_OTHER
#define CSCHED_OTHER SCHED_OTHER
#else
#define CSCHED_OTHER CSCHED_UNKNOWN
#endif
/* - Default (new primitive or non portable) */
#ifdef SCHED_FG_NP
#define CSCHED_FG_NP SCHED_FG_NP
#else
#define CSCHED_FG_NP CSCHED_UNKNOWN
#endif
/* - Background */
#ifdef SCHED_BG_NP
#define CSCHED_BG_NP SCHED_BG_NP
#else
#define CSCHED_BG_NP CSCHED_UNKNOWN
#endif

#else /* _CTHREAD */

/* Not a Thread Environment */
/* - First In, First Out */
#define CSCHED_FIFO CSCHED_UNKNOWN
/* - Round Robin */
#define CSCHED_RR CSCHED_UNKNOWN
/* - Default (other) */
#define CSCHED_OTHER CSCHED_UNKNOWN
/* - Default (new primitive or non portable) */
#define CSCHED_FG_NP CSCHED_UNKNOWN
/* - Background */
#define CSCHED_BG_NP CSCHED_UNKNOWN

#endif /* _CTHREAD */

/* Force the standard definition of scheduling parameter structure      */
/* This also ensures that programs interfaced with Cthread will compile */
/* on non-MT environments                                               */
struct Csched_param {
  int sched_priority;
};
typedef struct Csched_param Csched_param_t;

#endif /* __cthread_flags_h */











