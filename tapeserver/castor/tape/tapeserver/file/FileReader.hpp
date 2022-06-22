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

#include "scheduler/PositioningMethod.hpp"

namespace cta {
class RetrieveJob;
}

namespace castor {
namespace tape {
namespace tapeFile {

class ReadSession;
class UHL1;

class FileReader{
 public:
  /**
    * Constructor of the FileReader. It will bind itself to an existing read session
    * and position the tape right at the beginning of the file
    * @param rs: session to be bound to
    * @param fileInfo: information about the file we would like to read
    * @param positioningMode: method used when positioning (see the PositioningMode enum)
    */
  FileReader(const std::unique_ptr<ReadSession> &rs, const cta::RetrieveJob &fileToRecall);

  /**
    * Destructor of the FileReader. It will release the lock on the read session.
    */
  ~FileReader() throw();

  /**
    * After positioning at the beginning of a file for readings, this function
    * allows the reader to know which block sizes to provide.
    * @return the block size in bytes.
    */
  size_t getBlockSize();

  /**
    * Reads data from the file. The buffer should be equal to or bigger than the 
    * block size.
    * @param data: pointer to the data buffer
    * @param size: size of the buffer
    * @return The amount of data actually copied. Zero at end of file.
    */
  size_t read(void *data, const size_t size);

  /**
    * Returns the LBP access mode.
    * @return The LBP mode.
    */
  std::string getLBPMode();

 private:
  void positionByFseq(const cta::RetrieveJob &fileToRecall);
  void positionByBlockID(const cta::RetrieveJob &fileToRecall);
  /**
    * Positions the tape for reading the file. Depending on the previous activity,
    * it is the duty of this function to determine how to best move to the next
    * file. The positioning will then be verified (header will be read). 
    * As usual, exception is thrown if anything goes wrong.
    * @param fileInfo: all relevant information passed by the stager about the file.
    */
  void position(
    const cta::RetrieveJob &fileToRecall);

  /**
    * Set the block size member using the info contained within the uhl1
    * @param uhl1: the uhl1 header of the current file
    * @param fileInfo: the Information structure of the current file
    */
  void setBlockSize(const UHL1 &uhl1);

  /**
    * Block size in Bytes of the current file
    */
  size_t m_currentBlockSize;

  /**
    * Session to which we are attached to
    */
  const std::unique_ptr<ReadSession> &m_session;

  /**
    * What kind of command we use to position ourself on the tape (fseq or blockid)
    */
  cta::PositioningMethod m_positionCommandCode;

  /**
    * Description of the LBP mode with which the files is read.
    */
  std::string m_LBPMode;
};

}  // namespace tapeFile
}  // namespace tape
}  // namespace castor
