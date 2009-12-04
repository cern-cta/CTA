/******************************************************************************
 *                 castor/tape/aggregator/AggregatorMain.cpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/
 


#include "castor/exception/Internal.hpp"
#include "castor/server/TCPListenerThreadPool.hpp"
#include "castor/tape/aggregator_non_blocking/AggregatorDaemon.hpp"
#include "castor/tape/aggregator_non_blocking/AggregatorDlfMessageConstants.hpp"
#include "castor/tape/aggregator_non_blocking/DriveAllocationProtocolEngine.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/tape/aggregator_non_blocking/VdqmRequestHandler.hpp"


//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(int argc, char **argv) {

  castor::tape::aggregator::AggregatorDaemon daemon;

  return daemon.main(argc, argv);
}
