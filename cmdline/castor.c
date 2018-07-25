/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          Command-line tool for CTA Admin commands
 * @description    CTA Admin command using Google Protocol Buffers and XRootD SSI transport
 * @copyright      Copyright 2017 CERN
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

/* This is an "Easter egg": the former castor version tool */
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

#include "version.h"

int main(int argc, char ** argv) {
  int c;

  while ((c = getopt (argc, argv, "hv")) != EOF) {
    switch (c) {
    case 'v':
      printf ("CASTOR 3.0 - CTA %s\n", CTA_VERSION);
      break;
    case 'h':
    default:
      printf ("usage : %s [-v] [-h]\n", argv[0]);
      break;
    }
  }
  exit (0);
}
