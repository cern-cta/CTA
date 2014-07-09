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

#pragma once

#include "castor/tape/tpcp/Constants.hpp"
#include "castor/tape/tpcp/FilenameList.hpp"
#include "castor/tape/tpcp/TapeFseqRangeList.hpp"
#include "castor/utils/utils.hpp"
#include "h/Castor_limits.h"

#include <list>
#include <ostream>
#include <string>

namespace castor {
namespace tape   {
namespace tpcp   {

/**
 * Data type used to store the results of parsing the command-line.
 */
struct ParsedCommandLine {

  /**
   * True if the debug option has been set.
   */
  bool debugSet;

  /**
   * The filename of the "file list" file.
   */
  std::string fileListFilename;

  /**
   * True if the "file list" filename option has been set.
   */
  bool fileListSet;

  /**
   * The list filenames given on the command-line.
   */
  FilenameList filenames;

  /**
   * True if the help option has been set.
   */
  bool helpSet;

  /**
   * True if the tape server option has been set.
   */
  bool serverSet;

  /**
   * True if the nodata option has been set.
   */
  bool nodataSet;

  /**
   * The tape server to be used therefore overriding the drive scheduling of
   * the VDQM.
   */
  char server[CA_MAXHOSTNAMELEN+1];

  /**
   * The tape file sequence number ranges.
   */
  TapeFseqRangeList tapeFseqRanges;

  /**
   * The VID of the tape to be mounted.
   */
  char vid[CA_MAXVIDLEN+1];

  /**
   * Corresponds to the similarly named field of the RTCP_DUMPTAPE_REQ message.
   */
  int32_t dumpTapeMaxBytes;

  /**
   * Corresponds to the similarly named field of the RTCP_DUMPTAPE_REQ message.
   */
  int32_t dumpTapeBlockSize;

  /**
   * Corresponds to the similarly named field of the RTCP_DUMPTAPE_REQ message.
   */
  int32_t dumpTapeErrAction;

  /**
   * Corresponds to the similarly named field of the RTCP_DUMPTAPE_REQ message.
   */
  int32_t dumpTapeFromFile;

  /**
   * Corresponds to the similarly named field of the RTCP_DUMPTAPE_REQ message.
   */
  int32_t dumpTapeMaxFiles;

  /**
   * Corresponds to the similarly named field of the RTCP_DUMPTAPE_REQ message.
   */
  int32_t dumpTapeFromBlock;

  /**
   * Corresponds to the similarly named field of the RTCP_DUMPTAPE_REQ message.
   */
  int32_t dumpTapeToBlock;

  /**
   * Constructor.
   */
  ParsedCommandLine() :
    debugSet(false),
    fileListSet(false),
    helpSet(false),
    serverSet(false),
    nodataSet(false),
    dumpTapeMaxBytes(320),
    dumpTapeBlockSize(DEFAULTDUMPBLOCKSIZE),
    dumpTapeErrAction(-1),
    dumpTapeFromFile(1),
    dumpTapeMaxFiles(1),
    dumpTapeFromBlock(1),
    dumpTapeToBlock(1)
  {
    castor::utils::setBytes(server, '\0');
    castor::utils::setBytes(vid   , '\0');
  }
}; // struct ParsedCommandLine

} // namespace tpcp
} // namespace tape
} // namespace castor


