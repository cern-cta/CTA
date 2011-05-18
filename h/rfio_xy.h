/******************************************************************************
 *                      rfio_xy.h
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * header file for rfio_xy functions
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef H_RFIO_XY_H 
#define H_RFIO_XY_H 1

int rfio_xysock(int lun);

int rfio_xyopen(char* name, char* node, int lun,
                int lrecl, char* chopt, int* irc);

/* close a remote fortran logical unit  */
int rfio_xyclose(int lun, char* chopt, int *irc);

int rfio_xyread(int lun, char* buf, int nrec, int nwant,
                int* ngot, char* chopt, int* irc);

int rfio_xywrite(int lun, char* buf, int nrec, int nwrit,
                 char* chopt, int *irc);

#endif /* H_RFIO_XY_H */
