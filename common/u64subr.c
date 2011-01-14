/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

#include <stdio.h>       /* For sprintf */
#include <string.h>      /* For memset */
#include <ctype.h>       /* For isspace and al. */
#include <osdep.h>
#include "u64subr.h"

/* i64tostr - convert a signed 64 bits integer to a string
 *	buf must be long enough to contain the result
 *	if fldsize <= 0, the result is left adjusted in buf
 *	if fldsize > 0, the result is right adjusted
 *		leading spaces are inserted if needed
 */
char *i64tostr(signed64 i64,
               char *buf,
               int fldsize)
{
	char *p;
	u_signed64 u64;

	if (i64 >= 0)
		return (u64tostr ((u_signed64)i64, buf, fldsize));
	u64 = -i64;
	if (fldsize <= 0) {
		*buf = '-';
		(void) u64tostr (u64, buf + 1, fldsize);
	} else {
		(void) u64tostr (u64, buf, fldsize);
		p = buf;
		while (*p == ' ') p++;
		if (p > buf)
			*(p - 1) = '-';
	}
	return (buf);
}

/* i64tohexstr - convert a signed 64 bits integer to an hex string
 *	buf must be long enough to contain the result
 *	if fldsize <= 0, the result is left adjusted in buf
 *	if fldsize > 0, the result is right adjusted
 *		leading spaces are inserted if needed
 */
char *i64tohexstr(signed64 i64,
                  char *buf,
                  int fldsize)
{
	char *p;
	u_signed64 u64;

	if (i64 >= 0)
		return (u64tohexstr ((u_signed64)i64, buf, fldsize));
	u64 = -i64;
	if (fldsize <= 0) {
		*buf = '-';
		(void) u64tohexstr (u64, buf + 1, fldsize);
	} else {
		(void) u64tohexstr (u64, buf, fldsize);
		p = buf;
		while (*p == ' ') p++;
		if (p > buf)
			*(p - 1) = '-';
	}
	return (buf);
}

/* strtou64 - convert a string to an unsigned 64 bits integer */
u_signed64 strtou64(const char *str)
{
	char *p = (char *) str;
	u_signed64 u64 = 0;

	while (isspace (*p)) p++;	/* skip leading spaces */
	while (*p) {
		if (! isdigit (*p)) break;
		u64 = u64 * 10 + (*p++ - '0');
	}
	return (u64);
}

/* hexstrtou64 - convert an hex string to an unsigned 64 bits integer */
u_signed64 hexstrtou64(const char *str)
{
	char *p = (char *) str;
	u_signed64 u64 = 0;

	while (isspace (*p)) p++;	/* skip leading spaces */
	while (*p) {
		if ((! isdigit (*p)) &&
			(*p != 'A') && (*p != 'a') &&
			(*p != 'B') && (*p != 'b') &&
			(*p != 'C') && (*p != 'c') &&
			(*p != 'D') && (*p != 'd') &&
			(*p != 'E') && (*p != 'e') &&
			(*p != 'F') && (*p != 'f')
			) break;
		if (isdigit(*p)) {
			u64 = u64 * 10 + (*p++ - '0');
		} else {
			u64 = u64 * 16 + (*p++ - 'A' + 10);
		}
	}
	return (u64);
}

/* u64tostr - convert an unsigned 64 bits integer to a string */
/*	buf must be long enough to contain the result
 *	if fldsize <= 0, the result is left adjusted in buf
 *	if fldsize > 0, the result is right adjusted
 *		leading spaces are inserted if needed
 */
char *u64tostr(u_signed64 u64,
               char *buf,
               int fldsize)
{
	int n;
	u_signed64 t64;
	char *p;
	char tmpbuf[21];

	p = tmpbuf + 20;
	*p-- = '\0';

	do {
		t64 = u64 / 10;
		*p-- = '0' + (u64 - t64 * 10);
	} while ((u64 = t64));

	if (fldsize <= 0) {
		strcpy (buf, p + 1);
	} else {
		n = fldsize - (tmpbuf + 19 - p);
		if (n > 0) {
			memset (buf, ' ', n);
			strcpy (buf + n, p + 1);
		}
	}
		
	return (buf);
}

/* u64tohexstr - convert an unsigned 64 bits integer to an hex string
 *	buf must be long enough to contain the result
 *	if fldsize <= 0, the result is left adjusted in buf
 *	if fldsize > 0, the result is right adjusted
 *		leading spaces are inserted if needed
 */
char *u64tohexstr(u_signed64 u64,
                  char *buf,
                  int fldsize)
{
	int n;
	u_signed64 t64;
	char *p;
	char tmpbuf[17];

	p = tmpbuf + 16;
	*p-- = '\0';

	do {
		t64 = u64 / 16;
		switch (u64 - t64 * 16) {
		case 0:
		case 1:
		case 2:
		case 3:
		case 4:
		case 5:
		case 6:
		case 7:
		case 8:
		case 9:
			*p-- = '0' + (u64 - t64 * 16);
			break;
		case 10:
		case 11:
		case 12:
		case 13:
		case 14:
		case 15:
			*p-- = 'A' + (u64 - t64 * 16 - 10);
			break;
		}
	} while ((u64 = t64));

	if (fldsize <= 0) {
		strcpy (buf, p + 1);
	} else {
		n = fldsize - (tmpbuf + 15 - p);
		if (n > 0) {
			memset (buf, ' ', n);
			strcpy (buf + n, p + 1);
		}
	}
		
	return (buf);
}

/* strutou64 - convert a string with unit to an unsigned 64 bits integer */
u_signed64 strutou64(const char *str)
{
	char *p = (char *) str;
	u_signed64 u64 = 0;

	while (isspace (*p)) p++;	/* skip leading spaces */
	while (*p) {
		if (! isdigit (*p)) break;
		u64 = u64 * 10 + (*p++ - '0');
	}
	if (*p && ! *(p + 1)) {
		if (*p == 'K') u64 *= ONE_KB;
		else if (*p == 'M') u64 *= ONE_MB;
		else if (*p == 'G') u64 *= ONE_GB;
		else if (*p == 'T') u64 *= ONE_TB;
		else if (*p == 'P') u64 *= ONE_PB;
	}
	return (u64);
}

/* hexstrutou64 - convert an hex string with unit to an unsigned 64 bits integer */
u_signed64 hexstrutou64(const char *str)
{
	char *p = (char *) str;
	u_signed64 u64 = 0;

	while (isspace (*p)) p++;	/* skip leading spaces */
	while (*p) {
		if ((! isdigit (*p)) &&
			(*p != 'A') && (*p != 'a') &&
			(*p != 'B') && (*p != 'b') &&
			(*p != 'C') && (*p != 'c') &&
			(*p != 'D') && (*p != 'd') &&
			(*p != 'E') && (*p != 'e') &&
			(*p != 'F') && (*p != 'f')
			) break;
		if (isdigit(*p)) {
			u64 = u64 * 10 + (*p++ - '0');
		} else {
			u64 = u64 * 16 + (*p++ - 'A' + 10);
		}
	}
	if (*p && ! *(p + 1)) {
		if (*p == 'K') u64 *= ONE_KB;
		else if (*p == 'M') u64 *= ONE_MB;
		else if (*p == 'G') u64 *= ONE_GB;
		else if (*p == 'T') u64 *= ONE_TB;
		else if (*p == 'P') u64 *= ONE_PB;
	}
	return (u64);
}

/* u64tostru- gives the string representation with powers of 2 units of the
 * specified data-size, where the specified data-size is in bytes and is
 * represented by an unsigned 64-bit integer
 *	buf must be long enough to contain the result
 *	if fldsize <= 0, the result is left adjusted in buf
 *	if fldsize > 0, the result is right adjusted
 *		leading spaces are inserted if needed
 */
char *u64tostru(u_signed64 u64,
                char *buf,
                int fldsize)
{
	float fnum;
	int inum;
	int n;
	signed64 t64;
	char tmpbuf[21];
	char unit;

	t64 = u64;
	if (u64 >= ONE_PB) {
		fnum = (double) t64 / (double) ONE_PB;
		unit = 'P';
	} else if (u64 >= ONE_TB) {
		fnum = (double) t64 / (double) ONE_TB;
		unit = 'T';
	} else if (u64 >= ONE_GB) {
		fnum = (double) t64 / (double) ONE_GB;
		unit = 'G';
	} else if (u64 >= ONE_MB) {
		fnum = (double) t64 / (double) ONE_MB;
		unit = 'M';
	} else if (u64 >= ONE_KB) {
		fnum = (double) t64 / (double) ONE_KB;
		unit = 'K';
	} else {
		inum = (int) u64;
		unit = ' ';
	}
	if (unit != ' ')
		sprintf (tmpbuf, "%.2f%ci", fnum, unit); /* fnum <= 1024 */
	else
		sprintf (tmpbuf, "%d", inum); /* By constuction inum is < 1024 */

	if (fldsize <= 0) {
		strcpy (buf, tmpbuf);
	} else {
		n = fldsize - strlen (tmpbuf);
		if (n > 0) {
			memset (buf, ' ', n);
	        }
		strcpy (buf + n, tmpbuf);
	}
		
	return (buf);
}

/* u64tostri- gives the string representation with powers of 10 units
 * (SI units) of the specified data-size, where the specified data-size is in
 * bytes and is represented by an unsigned 64-bit integer
 *	buf must be long enough to contain the result
 *	if fldsize <= 0, the result is left adjusted in buf
 *	if fldsize > 0, the result is right adjusted
 *		leading spaces are inserted if needed
 */
char *u64tostrsi(u_signed64 u64,
                 char *buf,
                 int fldsize)
{
	float fnum;
	int inum;
	int n;
	signed64 t64;
	char tmpbuf[21];
	char unit;

	t64 = u64;
	if (u64 >= 1000000000000000LL) {
		fnum = (double) t64 / (double) 1000000000000000LL;
		unit = 'P';
	} else if (u64 >= 1000000000000LL) {
		fnum = (double) t64 / (double) 1000000000000LL;
		unit = 'T';
	} else if (u64 >= 1000000000) {
		fnum = (double) t64 / (double) 1000000000;
		unit = 'G';
	} else if (u64 >= 1000000) {
		fnum = (double) t64 / (double) 1000000;
		unit = 'M';
	} else if (u64 >= 1000) {
		fnum = (double) t64 / (double) 1000;
		unit = 'K';
	} else {
		inum = (int) u64;
		unit = ' ';
	}
	if (unit != ' ')
		sprintf (tmpbuf, "%.2f%c", fnum, unit);
	else
		sprintf (tmpbuf, "%d", inum);

	if (fldsize <= 0) {
		strcpy (buf, tmpbuf);
	} else {
		n = fldsize - strlen (tmpbuf);

                // If the content of tmpbuf fits exactly or requires padding
                if(n>=0) {
		    memset (buf, ' ', n);
		    strcpy (buf + n, tmpbuf);
                // Else the contents of tmpbuf is too much
                } else {
                    strncpy (buf, tmpbuf, fldsize);

                    // Ensure the string is termintaed
                    *(buf + fldsize - 1) = '\0';
                }
	}
		
	return (buf);
}
