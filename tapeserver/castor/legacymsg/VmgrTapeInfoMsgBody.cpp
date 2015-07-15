/******************************************************************************
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
 *
 * 
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/legacymsg/VmgrTapeInfoMsgBody.hpp"

#include <string.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::legacymsg::VmgrTapeInfoMsgBody::VmgrTapeInfoMsgBody() throw():
  nbSides(0),
  eTime(0),
  side(0),
  estimatedFreeSpace(0),
  nbFiles(0),
  rCount(0),
  wCount(0),
  rJid(0),
  wJid(0),
  rTime(0),
  wTime(0),
  status(0) {
  memset(vsn, '\0', sizeof(vsn));
  memset(library, '\0', sizeof(library));
  memset(dgn, '\0', sizeof(dgn));
  memset(density, '\0', sizeof(density));
  memset(labelType, '\0', sizeof(labelType));
  memset(model, '\0', sizeof(model));
  memset(mediaLetter, '\0', sizeof(mediaLetter));
  memset(manufacturer, '\0', sizeof(manufacturer));
  memset(serialNumber, '\0', sizeof(serialNumber));
  memset(poolName, '\0', sizeof(poolName));
  memset(rHost, '\0', sizeof(rHost));
  memset(wHost, '\0', sizeof(wHost));
}
