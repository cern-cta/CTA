/*
 * Copyright (C) 1993-1997 by CERN/CN/PDP/DH
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)packfseq.c	1.6 04/08/97 CERN CN-PDP/DH Jean-Philippe Baud";
#endif /* not lint */

#include <stdio.h>
#include <sys/types.h>
#include "stage.h"

packfseq(fseq_list, i, nbdskf, nbtpf, trailing, fseq, fseqsz)
fseq_elem *fseq_list;
int i;
int nbdskf;
int nbtpf;
char trailing;
char *fseq;
int fseqsz;
{
	char *dp;
	char fseq_tmp[MAXFSEQ];
	int j;
	int n1, n2;
	int prev;

	if (i < nbtpf)
		strcpy (fseq, (char *) (fseq_list + i));
	else
		strcpy (fseq, (char *) (fseq_list + nbtpf - 1));
	if ((i == nbdskf - 1) && (nbdskf < nbtpf)) {
		n1 = strtol (fseq_list + i, &dp, 10);
		prev = n1;
		for (j = i + 1; j < nbtpf; j++) {
			n2 = strtol (fseq_list + j, &dp, 10);
			if (n2 != prev && n2 != prev + 1) {
				if (prev > n1) {
					sprintf (fseq_tmp, "-%d", prev);
					if (strlen (fseq) + strlen (fseq_tmp) >= fseqsz)
						return (1);
					strcat (fseq, fseq_tmp);
				}
				sprintf (fseq_tmp, ",%d", n2);
				if (strlen (fseq) + strlen (fseq_tmp) >= fseqsz)
					return (1);
				strcat (fseq, fseq_tmp);
				n1 = n2;
			}
			prev = n2;
		}
		if (prev > n1 && trailing != '-') {
			sprintf (fseq_tmp, "-%d", prev);
			if (strlen (fseq) + strlen (fseq_tmp) >= fseqsz)
				return (1);
			strcat (fseq, fseq_tmp);
		}
	}
	if ((i == nbdskf - 1) && (trailing == '-')) {
		if (strlen (fseq) + 1 >= fseqsz)
			return (1);
		strcat (fseq, "-");
	}
	return (0);
}
