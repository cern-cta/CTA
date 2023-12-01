/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include <memory>
#include <string>

#include "castor/tape/tapeserver/daemon/VolumeInfo.hpp"
#include "castor/tape/tapeserver/file/Exceptions.hpp"

namespace castor::tape {

namespace tapeserver::drive {
class DriveInterface;
}

namespace tapeFile {

enum class PartOfFile {
  Header,
  HeaderProcessing,
  Payload,
  Trailer
};

/**
  * Class keeping track of a whole tape read session over an AUL formatted
  * tape. The session will keep track of the overall coherency of the session
  * and check for everything to be coherent. The tape should be mounted in
  * the drive before the AULReadSession is started (i.e. constructed).
  * Likewise, tape unmount is the business of the user.
  */
class ReadSession {
public:
  virtual ~ReadSession() = default;

  /**
    * DriveGeneric object referencing the drive used during this read session
    */
  tapeserver::drive::DriveInterface &m_drive;

  /**
    * Volume Serial Number
    */
  const std::string m_vid;

  /**
  * The boolean variable describing to use on not to use Logical
  * Block Protection.
  */
  const bool m_useLbp;

  inline void setCorrupted() noexcept {
    m_corrupted = true;
  }

  inline bool isCorrupted() noexcept {
    return m_corrupted;
  }

  inline bool isTapeWithLbp() noexcept {
    return m_detectedLbp;
  }

  inline void lock()  {
    if (m_locked) {
      throw SessionAlreadyInUse();
    }
    if (m_corrupted) {
      throw SessionCorrupted();
    }
    m_locked = true;
  }

  inline void release() noexcept {
    if (!m_locked) {
      m_corrupted = true;
    }
    m_locked = false;
  }

  inline void setCurrentFseq(uint32_t fseq) {
    m_fseq = fseq;
  }

  inline uint32_t getCurrentFseq() {
    return m_fseq;
  }

  inline const tapeserver::daemon::VolumeInfo& getVolumeInfo() {
    return m_volInfo;
  }

  inline void setCurrentFilePart(const PartOfFile& currentFilePart) {
    m_currentFilePart = currentFilePart;
  }

  inline PartOfFile getCurrentFilePart() {
    return m_currentFilePart;
  }

  inline std::string getLBPMode() {
    if (m_useLbp && m_detectedLbp)
      return "LBP_On";
    else if (!m_useLbp && m_detectedLbp)
      return "LBP_Off_but_present";
    else if (!m_detectedLbp)
      return "LBP_Off";
    throw cta::exception::Exception("In ReadSession::getLBPMode(): unexpected state");
  }

protected:
  /**
    * Constructor of the ReadSession. It will rewind the tape, and check the
    * volId value. Throws an exception in case of mismatch.
    * @param drive: drive object to which we bind the session
    * @param vid: volume name of the tape we would like to read from
    * @param useLbp: castor.conf option to use or not to use LBP in tapeserverd
    */
  ReadSession(tapeserver::drive::DriveInterface &drive,
          const tapeserver::daemon::VolumeInfo &volInfo,
          const bool useLbp);

  /**
    * set to true in case the destructor of ReadFile finds a missing lock on its session
    */
  bool m_corrupted;

  /**
    * Session lock to be sure that a read session is owned by maximum one ReadFile object
    */
  bool m_locked;

  /**
    * Current fSeq, used only for positioning by fseq
    */
  uint32_t m_fseq;

  /**
    * Part of the file we are reading
    */
  PartOfFile m_currentFilePart;

  const tapeserver::daemon::VolumeInfo m_volInfo;

  /**
  * The boolean variable indicates that the tape has VOL1 with enabled LBP
  */
  bool m_detectedLbp;
};

}} // namespace castor::tape::tapeFile
