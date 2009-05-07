/******************************************************************************
 *                 castor/tape/tpcp/TpcpCommand.cpp
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
 
 
#include "castor/tape/tpcp/TpcpCommand.hpp"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::TpcpCommand::TpcpCommand(): m_callbackSocket(false) {
  // Do nothing
}


//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int castor::tape::tpcp::TpcpCommand::main(const int argc, char **argv) {
/*
  try {

  } catch (castor::exception::Exception &ex) {
    std::cerr << std::endl << "Failed to start daemon: "
      << ex.getMessage().str() << std::endl << std::endl;
    castor::tape::aggregator::AggregatorDaemon::usage(std::cerr);
    std::cerr << std::endl;
    return 1;
  }
*/
  return 0;
}
