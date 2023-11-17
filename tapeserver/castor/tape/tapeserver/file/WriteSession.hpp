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

#include <string>

#include "castor/tape/tapeserver/daemon/VolumeInfo.hpp"

namespace castor::tape {

namespace tapeserver::drive {
class DriveInterface;
}

namespace tapeFile {

/**
  * Class keeping track of a whole tape write session over an AUL formatted
  * tape. The session will keep track of the overall coherency of the session
  * and check for everything to be coherent. The tape should be mounted in
  * the drive before the WriteSession is started (i.e. constructed).
  * Likewise, tape unmount is the business of the user.
  */
class WriteSession{
public:
  /**
    * Constructor of the WriteSession. It will rewind the tape, and check the
    * VSN value. Throws an exception in case of mismatch. Then positions the tape at the
    * end of the last file (actually at the end of the last trailer) after having checked
    * the fseq in the trailer of the last file.
    * @param drive: drive object to which we bind the session
    * @param volId: volume name of the tape we would like to write to
    * @param last_fseq: fseq of the last active (undeleted) file on tape
    * @param compression: set this to true in case the drive has compression enabled (x000GC)
    * @param useLbp: castor.conf option to use or not to use LBP in tapeserverd
    */
  WriteSession(tapeserver::drive::DriveInterface &drive,
    const tapeserver::daemon::VolumeInfo &volInfo,
    const uint32_t last_fseq, const bool compression,
    const bool useLbp);

  /**
    * DriveGeneric object referencing the drive used during this write session
    */
  tapeserver::drive::DriveInterface & m_drive;

  /**
    * Volume Serial Number
    */
  const std::string m_vid;

  /**
    * set to true if the drive has compression enabled
    */
  bool m_compressionEnabled;

  /**
  * The boolean variable describing to use on not to use Logical
  * Block Protection.
  */
  const bool m_useLbp;

  inline std::string getSiteName() noexcept {
    return m_siteName;
  }

  inline std::string getHostName() noexcept {
    return m_hostName;
  }

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

  inline void release() {
    if (!m_locked) {
      m_corrupted = true;
      throw SessionCorrupted();
    }
    m_locked = false;
  }

  inline const tapeserver::daemon::VolumeInfo& getVolumeInfo()  const {
    return m_volInfo;
  }

  /**
    * Checks that a fSeq we are intending to write the the proper one,
    * following the previous written one in sequence.
    * This is to be used by the tapeWriteTask right before writing the file
    * @param nextFSeq The fSeq we are about to write.
    */
  inline void validateNextFSeq(uint64_t nextFSeq) const {
    if (nextFSeq != m_lastWrittenFSeq + 1) {
      throw cta::exception::Exception("In WriteSession::validateNextFSeq: wrong fSeq sequence: lastWrittenFSeq="
                                      + std::to_string(m_lastWrittenFSeq) + " nextFSeq=" + std::to_string(nextFSeq));
    }
  }

  /**
    * Checks the value of the lastfSeq written to tape and records it.
    * This is to be used by the tape write task right after closing the
    * file.
    * @param writtenFSeq the fSeq of the file
    */
  inline void reportWrittenFSeq(uint64_t writtenFSeq) {
    if (writtenFSeq != m_lastWrittenFSeq + 1) {
      throw cta::exception::Exception("In WriteSession::reportWrittenFSeq: wrong fSeq reported: lastWrittenFSeq="
                                      + std::to_string(m_lastWrittenFSeq)
                                      + " writtenFSeq=" + std::to_string(writtenFSeq));
    }
    m_lastWrittenFSeq = writtenFSeq;
  }

  /**
    * Gets the LBP mode for logs
    */
  std::string getLBPMode();

private:
  /**
    * looks for the site name in /etc/resolv.conf in the search line and saves the upper-cased value in siteName
    */
  void setSiteName();

  /**
    * uses gethostname to get the upper-cased hostname without the domain name
    */
  void setHostName();

  /**
    * The following two variables are needed when writing the headers and trailers, sitename is grabbed from /etc/resolv.conf
    */
  std::string m_siteName;

  /**
    * hostname is instead gotten from gethostname()
    */
  std::string m_hostName;

  /**
    * keep track of the fSeq we are writing to tape
    */
  uint64_t m_lastWrittenFSeq;

  /**
    * set to true in case the write operations do (or try to do) something illegal
    */
  bool m_corrupted;

  /**
    * Session lock to be sure that a read session is owned by maximum one WriteFile object
    */
  bool m_locked;

  const tapeserver::daemon::VolumeInfo m_volInfo;

  /**
  * The boolean variable indicates that the tape has VOL1 with enabled LBP
  */
  bool m_detectedLbp;

  const uint16_t MAX_UNIX_HOSTNAME_LENGTH = 256;  // 255 + 1 terminating character
};

}} // namespace castor::tape::tapeFile
