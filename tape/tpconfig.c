/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */


/*	tpconfig - configure tape drive up/down */
#include <stdlib.h>
#include <errno.h>
#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "sacct.h"
#include "serrno.h"
struct confrsn {
	char *text;
	int code;
};
 
struct confrsn confdnrsn[] = {
	{"cleaning",	TPCD_CLN},
	{"test",	TPCD_TST},
	{"failure",	TPCD_HWF},
	{"system",	TPCD_SYS},
	{"suspect",	TPCD_SUS},
	{"upgrade",	TPCD_UPG},
	{"ops",		TPCD_OPS}
};
struct confrsn confuprsn[] = {
	{"cleaned",	TPCU_CLN},
	{"replaced",	TPCU_RPL},
	{"repaired",	TPCU_RPR},
	{"preventive",	TPCU_PRV},
	{"upgraded",	TPCU_UPG},
        {"ops",	        TPCU_OPS}
};

static void tpconfig_usage(char *cmd)
{
	fprintf (stderr, "usage: %s ", cmd);
	fprintf (stderr, "unit_name status\n");
}

int main(int	argc,
         char	**argv)
{
	int reason;
	int status;

	if (argc != 3) {
		fprintf (stderr, "Wrong number of command-line arguments"
			": expected 2, actual was %d\n", (argc - 1));
		tpconfig_usage (argv[0]);
		exit (USERR);
	}
	if (strcmp (argv[2], "up") == 0) {
		status = CONF_UP;
		reason = TPCU_OPS;
	} else if (strcmp (argv[2], "down") == 0) {
		status = CONF_DOWN;
		reason = TPCD_OPS;
	} else {
		fprintf (stderr, "Invalid status"
			": expected up or down, actual was %s\n", argv[2]);
		tpconfig_usage (argv[0]);
		exit (USERR);
	}
	if (Ctape_config (argv[1], status, reason) < 0) {
		fprintf (stderr, TP009, argv[1], sstrerror(serrno));
		if (serrno == EINVAL || serrno == ETIDN) {
			exit (USERR);
		} else {
			exit (SYERR);
		}
	} else {
		exit (0);
	}
}


