/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: tpconfig.c,v $ $Revision: 1.1 $ $Date: 1999/09/20 12:27:44 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	tpconfig - configure tape drive up/down */
#include <stdio.h>
#include <sys/types.h>
#include "Ctape.h"
#include "Ctape_api.h"
#if SACCT
#include "sacct.h"
struct confrsn {
	char *text;
	int code;
};
 
struct confrsn confdnrsn[] = {
	"cleaning",	TPCD_CLN,
	"test",		TPCD_TST,
	"failure",	TPCD_HWF,
	"system",	TPCD_SYS,
	"suspect",	TPCD_SUS,
	"upgrade",	TPCD_UPG,
	"ops",		TPCD_OPS
};
struct confrsn confuprsn[] = {
	"cleaned",	TPCU_CLN,
	"replaced",	TPCU_RPL,
	"repaired",	TPCU_RPR,
	"preventive",	TPCU_PRV,
	"upgraded",	TPCU_UPG,
	"ops",		TPCU_OPS
};
#endif

main(argc, argv)
int	argc;
char	**argv;
{
	int c, n;
	int reason;
	int status;

	if (argc != 3 && argc != 4) {
		usage (argv[0]);
		exit (USERR);
	}
	if (strcmp (argv[2], "up") == 0)
		status = CONF_UP;
	else if (strcmp (argv[2], "down") == 0)
		status = CONF_DOWN;
	else {
		usage (argv[0]);
		exit (USERR);
	}
#if SACCT
	if (argc == 4) {
		if (status == CONF_UP) {
			for (n = 0; n < sizeof(confuprsn)/sizeof(struct confrsn); n++)
				if (strcmp (argv[3], confuprsn[n].text) == 0) break;
			if (n == sizeof(confuprsn)/sizeof(struct confrsn)) {
				fprintf (stderr, TP059);
				exit (USERR);
			}
			reason = confuprsn[n].code;
		} else {
			for (n = 0; n < sizeof(confdnrsn)/sizeof(struct confrsn); n++)
				if (strcmp (argv[3], confdnrsn[n].text) == 0) break;
			if (n == sizeof(confdnrsn)/sizeof(struct confrsn)) {
				fprintf (stderr, TP059);
				exit (USERR);
			}
			reason = confdnrsn[n].code;
		}
	}
#endif
	c = Ctape_config (argv[1], status, reason);
	exit (c);
}

usage(cmd)
char *cmd;
{
	fprintf (stderr, "usage: %s ", cmd);
	fprintf (stderr, "unit_name status\n");
}

