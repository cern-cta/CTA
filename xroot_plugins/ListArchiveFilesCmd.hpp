/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "catalogue/Catalogue.hpp"
#include "catalogue/TapeFileSearchCriteria.hpp"
#include "common/log/Logger.hpp"

#include <XrdSfs/XrdSfsInterface.hh>
#include <sstream>
#include <string>

namespace cta {
namespace xrootPlugins {

/**
 * Server-side class implementing the listing of one or more archive files.
 */
class ListArchiveFilesCmd {
public:

  /**
   * Constructor.
   *
   * @param log The object representing the API of the CTA logging system.
   * @param xrdSfsFileError The error member-variable of the XrdSfsFile class.
   * @param displayHeader Set to true if the user requested the header to be
   * displayed.
   * @param archiveFileItor Iterator over the archive files in the CTA catalogue that are to be listed.
   */
  ListArchiveFilesCmd(
    log::Logger &log,
    XrdOucErrInfo &xrdSfsFileError,
    const bool displayHeader,
    catalogue::ArchiveFileItor archiveFileItor);

  /**
   * Indirectly implements the XrdSfsFile::read() method.
   */
  XrdSfsXferSize read(XrdSfsFileOffset offset, char *buffer, XrdSfsXferSize size);

  /**
   * Indirectly implements the XrdSfsFile::read() method.
   */
  XrdSfsXferSize exceptionThrowingRead(XrdSfsFileOffset offset, char *buffer, XrdSfsXferSize size);
  
protected:

  /**
   * The object representing the API of the CTA logging system.
   */
  log::Logger &m_log;

  /**
   * The error member-variable of the XrdSfsFile class.
   */
  XrdOucErrInfo &m_xrdSfsFileError;

  /**
   * Set to true if the user requested the columns header to be display.
   */
  bool m_displayHeader = false;

  /**
   * Enumeration of the possible states of the "list archive files" command.
   */
  enum State {
    WAITING_FOR_FIRST_READ,
    LISTING_ARCHIVE_FILES,
    LISTED_LAST_ARCHIVE_FILE};

  /**
   * Returns the string representation of the specified state.
   * @param state The state.
   * @return The string representation of the state.
   */
  std::string stateToStr(const State state) const;

  /**
   * The current state of the "list archive files" command.
   */
  State m_state = WAITING_FOR_FIRST_READ;

  /**
   * Iterator over the archive files in the CTA catalogue that are to be listed.
   */
  catalogue::ArchiveFileItor m_archiveFileItor;

  /**
   * The buffer that sits between calls to ListArchiveFilesCmd::read() and calls
   * to the CTA catalogue.
   *
   * The XRootD server calls XrdCtaFile::read() which in turn calls
   * ListArchiveFilesCmd::read() which in turn calls the CTA catalogue.  Both
   * read() method are byte oriented whereas the CTA catalogue is record
   * oriented.  This buffer ties the byte oriented read() calls to the records
   * of the CTA catalogue.
   *
   */
  std::ostringstream m_readBuffer;

  /**
   * The offset in the reply file at which the current m_readBuffer starts.
   */
  XrdSfsFileOffset m_fileOffsetOfReadBuffer = 0;

  /**
   * The file offset expected in the next call to ListArchiveFilesCmd::read();
   */
  XrdSfsFileOffset m_expectedFileOffset = 0;

  /**
   * Refreshes the read buffer by first clearing it and then attempting to write
   * into it more records from the CTA catalogue.
   */
  void refreshReadBuffer();
}; // class ListArchiveFilesCmd

} // namespace xrootPlugins
} // namespace cta
