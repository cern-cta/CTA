/******************************************************************************
 *                 test/castor/tape/tpcp/testtpcp.cpp
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
 
#include "castor/tape/tapegateway/FileRecalledNotification.hpp"
#include "castor/tape/tapegateway/FileToRecall.hpp"
#include "castor/tape/tapegateway/FileToRecallRequest.hpp"
#include "castor/tape/tpcp/StreamOperators.hpp"
#include "castor/tape/utils/utils.hpp"

#include <iostream>


//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(int argc, char **argv) {

  using namespace castor::tape;

  std::ostream &os = std::cout;

  tapegateway::FileToRecallRequest fileToRecallRequest;
  utils::writeBanner(os, "fileToRecallRequest");
  os << std::endl
     << fileToRecallRequest << std::endl;

  castor::tape::tapegateway::FileToRecall fileToRecall;
  os << std::endl;
  utils::writeBanner(os, "fileToRecall");
  os << std::endl
     << fileToRecall << std::endl;

  tapegateway::FileRecalledNotification fileRecalledNotification;
  os << std::endl;
  utils::writeBanner(os, "fileRecalledNotification");
    os << std::endl
     << fileRecalledNotification << std::endl;

  return 0;
}
