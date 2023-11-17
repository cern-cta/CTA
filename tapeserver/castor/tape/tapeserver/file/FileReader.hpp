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

namespace castor::tape::tapeFile {

class ReadSession;

class FileReader {
public:
  /**
    * Destructor of the FileReader. It will release the lock on the read session.
    */
  virtual ~FileReader() throw();

  /**
    * After positioning at the beginning of a file for readings, this function
    * allows the reader to know which block sizes to provide.
    * @return the block size in bytes.
    */
  size_t getBlockSize();

  /**
    * Reads the next data block from the file. The buffer should be equal to or bigger than the
    * block size.
    * It has to throw EndOfFile() exception when it reache the End Of File.
    * @param data: pointer to the data buffer
    * @param size: size of the buffer
    * @return The amount of data actually copied. Zero at end of file.
    */
  virtual size_t readNextDataBlock(void *data, const size_t size) = 0;

  /**
    * Returns the LBP access mode.
    * @return The LBP mode.
    */
  std::string getLBPMode();

  friend class FileReaderFactory;

protected:
  /**
    * Constructor of the FileReader. It will bind itself to an existing read session
    * and position the tape right at the beginning of the file
    * @param rs: session to be bound to
    * @param fileInfo: information about the file we would like to read
    * @param positioningMode: method used when positioning (see the PositioningMode enum)
    */
  FileReader(const std::unique_ptr<ReadSession> &rs, const cta::RetrieveJob &fileToRecall);

  /**
    * Move throught the headers to the block to read using Fseq Delta increments.
    */
  virtual void positionByFseq(const cta::RetrieveJob &fileToRecall) = 0;

  /**
    * Move throught the headers to the blocks to read using the ID of the block.
    */
  virtual void positionByBlockID(const cta::RetrieveJob &fileToRecall) = 0;

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

private:
  /**
    * Description of the LBP mode with which the files is read.
    */
  std::string m_LBPMode;

  /**
    * Positions the tape for reading the file. Depending on the previous activity,
    * it is the duty of this function to determine how to best move to the next
    * file. The positioning will then be verified (header will be read).
    * As usual, exception is thrown if anything goes wrong.
    * @param fileInfo: all relevant information passed by the stager about the file.
    */
  void position(const cta::RetrieveJob &fileToRecall);
};

} // namespace castor::tape::tapeFile
