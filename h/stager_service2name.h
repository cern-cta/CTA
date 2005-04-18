/* $Id: stager_service2name.h,v 1.4 2005/04/18 09:08:02 jdurand Exp $ */

#ifndef __stager_service2name_h
#define __stager_service2name_h

char *service2name[STAGER_SERVICE_MAX] = { "STAGER_SERVICE_DB",
					   "STAGER_SERVICE_QUERY",
					   "STAGER_SERVICE_GETNEXT",
					   "STAGER_SERVICE_JOB",
					   "STAGER_SERVICE_GC",
					   "STAGER_SERVICE_ERROR"
};

#endif /* __stager_service2name_h */
