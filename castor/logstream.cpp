/******************************************************************************
 *                      logstream.cpp
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
 * @(#)$RCSfile: logstream.cpp,v $ $Revision: 1.4 $ $Release$ $Date: 2004/07/12 14:19:03 $ $Author: sponcec3 $
 *
 * A generic logstream for castor, handling IP addresses
 * and timestamps
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "logstream.h"

#define MANIPULATORIMPL(T)                      \
  castor::logstream& castor::T(castor::logstream& s) {  \
    s.setLevel(castor::logbuf::T);           \
    return s;                                   \
  }

MANIPULATORIMPL(VERBOSE);
MANIPULATORIMPL(DEBUG);
MANIPULATORIMPL(INFO);
MANIPULATORIMPL(WARNING);
MANIPULATORIMPL(ERROR);
MANIPULATORIMPL(FATAL);
MANIPULATORIMPL(ALWAYS);

castor::logstream& castor::ip(castor::logstream& s) {
  s.setIsIP(true);
  return s;
}

castor::logstream& castor::timeStamp(castor::logstream& s) {
  s.setIsTimeStamp(true);
  return s;
}
