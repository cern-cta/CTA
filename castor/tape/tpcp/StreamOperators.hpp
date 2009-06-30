/******************************************************************************
 *                 castor/tape/tpcp/StreamOperators.hpp
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

#ifndef CASTOR_TAPE_TPCP_STREAMOPERATORS_HPP
#define CASTOR_TAPE_TPCP_STREAMOPERATORS_HPP 1

#include "castor/tape/tapegateway/FileRecalledNotification.hpp"
#include "castor/tape/tapegateway/FileToRecall.hpp"
#include "castor/tape/tapegateway/FileToRecallRequest.hpp"
#include "castor/tape/tpcp/Action.hpp"
#include "castor/tape/tpcp/FilenameList.hpp"
#include "castor/tape/tpcp/ParsedCommandLine.hpp"
#include "castor/tape/tpcp/TapeFseqRange.hpp"
#include "castor/tape/tpcp/TapeFseqRangeList.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/vmgr_api.h"

//#include <list>
//#include <ostream>
//#include <string>

using namespace castor::tape;

/**
 * ostream << operator for castor::tape::tpcp::Action
 */
std::ostream &operator<<(std::ostream &os,
  const castor::tape::tpcp::Action &action);


/**
 * ostream << operator for castor::tape::tpcp::FilenameList
 */
std::ostream &operator<<(std::ostream &os,
  const castor::tape::tpcp::FilenameList &list);


/**
 * ostream << operator for castor::tape::tpcp::ParsedCommandLine
 */
std::ostream &operator<<(std::ostream &os,
  const castor::tape::tpcp::ParsedCommandLine &cmdLine);


/**
 * ostream << operator for castor::tape::tpcp::TapeFseqRange
 */
std::ostream &operator<<(std::ostream &os,
  const castor::tape::tpcp::TapeFseqRange &range);


/**
 * ostream << operator for castor::tape::tpcp::TapeFseqRangeList
 */
std::ostream &operator<<(std::ostream &os,
  const castor::tape::tpcp::TapeFseqRangeList &list);


/**
 * ostream << operator for vmgr_tape_info structure 
 */
std::ostream &operator<<(std::ostream &os,
  const vmgr_tape_info &FileToRecallRequest);


/**
 * ostream << operator for tapegateway::fileToRecall object
 */
std::ostream &operator<<(std::ostream &os,
  const tapegateway::FileToRecall &fileToRecallInfo);

/**
 * ostream << operator for tapegateway::fileToRecallRequest object
 */
std::ostream &operator<<(std::ostream &os,
  const tapegateway::FileToRecallRequest &fileToRecallRequestInfo);


/**
 * ostream << operator for tapegateway::FileRecalledNotification object 
 */
std::ostream &operator<<(std::ostream &os,
  const tapegateway::FileRecalledNotification &notificationInfo);




#endif // CASTOR_TAPE_TPCP_STREAMOPERATORS_HPP
