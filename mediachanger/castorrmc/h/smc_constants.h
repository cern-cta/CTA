/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 1998-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

			/* error messages */

#define	SR001	"SR001 - drive ordinal must be a non negative integer\n"
#define	SR002	"SR002 - option -%c and -%c are mutually exclusive\n"
#define	SR003	"SR003 - invalid query type %c\n"
#define	SR004	"SR004 - vid %s must be at most 6 characters long\n"
#define	SR005	"SR005 - loader must be specified\n"
#define	SR006	"SR006 - drive ordinal is mandatory for demount operations\n"
#define	SR007	"SR007 - drive ordinal and vid are mandatory for mount operations\n"
#define	SR008	"SR008 - invalid device ordinal (must be < %d)\n"
#define	SR009	"SR009 - vid mismatch: %s on request, %s on drive\n"
#define	SR010	"SR010 - number of elements must be a positive integer\n"
#define	SR011	"SR011 - vid is mandatory for export operations\n"
#define	SR012	"SR012 - cannot allocate enough memory\n"
#define	SR013	"SR013 - export slots are full\n"
#define	SR014	"SR014 - slot ordinal must be a non negative integer\n"
#define	SR015	"SR015 - storage cells are full\n"
#define	SR016	"SR016 - invalid slot address (must be < %d)\n"
#define	SR017	"SR017 - %s %s failed : %s\n"
#define	SR018	"SR018 - %s of %s on drive %d failed : %s\n"
#define	SR019	"SR019 - %s : %s error : %s\n"
#define	SR020	"SR020 - %s failed : %s\n"
#define	SR021	"SR021 - specify source slot and target slot\n"
