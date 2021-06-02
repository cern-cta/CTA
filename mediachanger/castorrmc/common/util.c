/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <ctype.h>
#include <osdep.h>

/**
 * Checks whether the given string is a size.
 * The accepted syntax is :
 *    [blanks]<digits>[b|k|M|G|T|P]i?
 * @return -1 if it is not a size,
 * 0 if it is a size with no unit,
 * 1 if it is a size with a unit
 */
int check_for_strutou64(char *str) {
  /* We accept only the following format */
  /* [blanks]<digits>[b|k|M|G|T|P]i? */
  char *p = (char *) str;
  while (isspace (*p)) p++; /* skip leading spaces */
  while (*p) {
    if (! isdigit (*p)) break;
    p++;
  }

  /* End of the digits section:  */
  /* Either there is a supported unit, either there is none */
  switch (*p) {
  case '\0':
    return(0); /* Ok and no unit */
  case 'b':  /* Not supported by u64
                we say it is bytes
                u64 will ignore it, simply */
  case 'k':
  case 'M':
  case 'G':
  case 'T':
  case 'P':
    switch (*++p) {
    case '\0':
      return(1); /* Ok with unit */
    case 'i':
      if (*(p+2) == '\0')
        return(1); /* Ok with power of 2 unit */
      else
        return(-1); /* Unit followed by anything, not valid */
    default:
      return(-1); /* Unit followed by anything, not valid */
    }
  default:
    return(-1); /* Unknown unit */
  }
}
