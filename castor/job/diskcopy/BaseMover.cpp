/******************************************************************************
 *                      BaseMover.cpp
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
 * @(#)$RCSfile: BaseMover.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2007/11/26 14:54:54 $ $Author: waldron $
 *
 * @author Dennis Waldron
 *****************************************************************************/

// Include files
#include "castor/job/diskcopy/BaseMover.hpp"


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::job::diskcopy::BaseMover::BaseMover() :
  m_shutdown(0),
  m_fileSize(0),
  m_bytesTransferred(0) {

}
