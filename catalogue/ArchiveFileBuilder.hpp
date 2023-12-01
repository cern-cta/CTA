/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "common/dataStructures/ArchiveFile.hpp"
#include "common/log/Logger.hpp"

#include <memory>

namespace cta::catalogue {

/**
 * Builds ArchiveFile objects from a stream of tape files ordered by archive ID
 * and then copy number.
 * The template T is an ArchiveFile or a DeletedArchiveFile
 */
template<typename T>
class ArchiveFileBuilder {
public:

  /**
   * Constructor.
   *
   * @param log Object representing the API to the CTA logging system.
   */
  explicit ArchiveFileBuilder(log::Logger &log);

  /**
   * Appends the specified tape file to the ArchiveFile object currently
   * construction.
   *
   * If this append method is called with the tape file of the next ArchiveFile
   * to be constructed then this means the current ArchiveFile under
   * construction is complete and this method will therefore return the current
   * and complete ArchiveFile object.  The appened tape file will be remembered
   * by this builder object and used to start the construction of the next
   * ArchiveFile object.
   *
   * If this append method is called with an ArchiveFile with no tape files at
   * all then this means the current ArchiveFile under
   * construction is complete and this method will therefore return the current
   * and complete ArchiveFile object.  The appened tape file will be remembered
   * by this builder object and used to start the construction of the next
   * ArchiveFile object.
   *
   * If the call to this append does not complete the ArchiveFile object
   * currently under construction then this method will returns an empty unique
   * pointer.
   *
   * @param tapeFile The tape file to be appended or an archive file with no
   * tape files at all.
   */
  std::unique_ptr<T> append(const T &tapeFile);

  /**
   * Returns a pointer to the ArchiveFile object currently under construction.
   * A return value of nullptr means there there is no ArchiveFile object
   * currently under construction.
   *
   * @return The ArchiveFile object currently under construction or nullptr
   * if there isn't one.
   */
  T *getArchiveFile();

  /**
   * If there is an ArchiveFile under construction then it is forgotten.
   */
  void clear();

private:

  /**
   * Object representing the API to the CTA logging system.
   */
  log::Logger &m_log;

  /**
   * The Archivefile object currently under construction.
   */
  std::unique_ptr<T> m_archiveFile;

}; // class ArchiveFileBuilder

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
template<typename T>
ArchiveFileBuilder<T>::ArchiveFileBuilder(log::Logger &log):
  m_log(log) {
}

//------------------------------------------------------------------------------
// append
//------------------------------------------------------------------------------
template<typename T>
std::unique_ptr<T> ArchiveFileBuilder<T>::append(
  const T &tapeFile) {

  // If there is currently no ArchiveFile object under construction
  if(nullptr == m_archiveFile.get()) {
    // If the tape file represents an ArchiveFile object with no tape files
    if(tapeFile.tapeFiles.empty()) {
      // Archive file is already complete
      return std::unique_ptr<T>(new T(tapeFile));
    }

    // If the tape file exists then it must be alone
    if(tapeFile.tapeFiles.size() != 1) {
      exception::Exception ex;
      ex.getMessage() << __FUNCTION__ << " failed: Expected exactly one tape file to be appended at a time: actual=" <<
        tapeFile.tapeFiles.size();
      throw ex;
    }

    // Start constructing one
    m_archiveFile.reset(new T(tapeFile));

    // There could be more tape files so return incomplete
    return std::unique_ptr<T>();
  }

  // If the tape file represents an ArchiveFile object with no tape files
  if(tapeFile.tapeFiles.empty()) {
    // The ArchiveFile object under construction is complete,
    // therefore return it and start the construction of the next
    std::unique_ptr<T> tmp;
    tmp = std::move(m_archiveFile);
    m_archiveFile.reset(new T(tapeFile));
    return tmp;
  }

  // If the tape file to be appended belongs to the ArchiveFile object
  // currently under construction
  if(tapeFile.archiveFileID == m_archiveFile->archiveFileID) {

    // The tape file must exist and must be alone
    if(tapeFile.tapeFiles.size() != 1) {
      exception::Exception ex;
      ex.getMessage() << __FUNCTION__ << " failed: Expected exactly one tape file to be appended at a time: actual=" <<
        tapeFile.tapeFiles.size() << " archiveFileID=" << tapeFile.archiveFileID;
      throw ex;
    }

    // Append the tape file
    m_archiveFile->tapeFiles.push_back(tapeFile.tapeFiles.front());

    // There could be more tape files so return incomplete
    return std::unique_ptr<T>();
  }

  // Reaching this point means the tape file to be appended belongs to the next
  // ArchiveFile to be constructed.

  // ArchiveFile object under construction is complete,
  // therefore return it and start the construction of the next
  std::unique_ptr<T> tmp;
  tmp = std::move(m_archiveFile);
  m_archiveFile.reset(new T(tapeFile));
  return tmp;
}

//------------------------------------------------------------------------------
// getArchiveFile
//------------------------------------------------------------------------------
template<typename T>
T *ArchiveFileBuilder<T>::getArchiveFile() {
  return m_archiveFile.get();
}

//------------------------------------------------------------------------------
// clear
//------------------------------------------------------------------------------
template<typename T>
void ArchiveFileBuilder<T>::clear() {
  m_archiveFile.reset();
}


} // namespace cta::catalogue
