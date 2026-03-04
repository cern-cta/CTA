/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <memory>
#include <string>

namespace cta {
class ArchiveJob;
}

namespace castor::tape::tapeFile {

class WriteSession;

class FileWriter {
public:
  /**
    * Constructor of the FileWriter. It will bind itself to an existing write session
    * and position the tape right at the end of the last file
    * @param ws: session to be bound to
    * @param fileInfo: information about the file we want to read
    * @param blockSize: size of blocks we want to use in writing
    */
  FileWriter(WriteSession& ws, const cta::ArchiveJob& fileToMigrate, const size_t blockSize);

  /**
    * Returns the block id of the current position
    * @return blockId of current position
    */
  uint32_t getPosition();

  /**
    * Retuns the block is of the first block of the header of the file.
    * This is changed from 1 to 0 for the first file on the tape (fseq=1)
    * @return blockId of the first tape block of the file's header.
    */
  uint32_t getBlockId();

  /**
    * Get the block size (that was set at construction time)
    * @return the block size in bytes.
    */
  size_t getBlockSize();

  /**
    * Writes a block of data on tape
    * @param data: buffer to copy the data from
    * @param size: size of the buffer
    */
  void write(const void* data, const size_t size);

  /**
    * Closes the file by writing the corresponding trailer on tape
    * HAS TO BE CALL EXPLICITLY
    */
  void close();

  /**
    * Destructor of the FileWriter object. Releases the WriteSession
    */
  ~FileWriter() noexcept;

  /**
    * Returns the LBP access mode.
    * @return The LBP mode.
    */
  std::string getLBPMode();

private:
  /**
    * Block size in Bytes of the current file
    */
  size_t m_currentBlockSize;

  /**
    * Session to which we are attached to
    */
  WriteSession& m_session;

  /**
    * Information that we have about the current file to be written and that
    * will be used to write appropriate headers and trailers
    */
  const cta::ArchiveJob& m_fileToMigrate;

  /**
    * set to true whenever the constructor is called and to false when close() is called
    */
  bool m_open = false;

  /**
    * set to false initially, set to true after at least one successful nonzero writeBlock operation
    */
  bool m_nonzeroFileWritten = false;

  /**
    * number of blocks written for the current file
    */
  int m_numberOfBlocks = 0;

  /**
    * BlockId of the file (tape block id of the first header block).
    * This value is retried at open time.
    */
  u_int32_t m_blockId;

  /**
    * Description of the LBP mode with which the files is read.
    */
  std::string m_LBPMode;
};

}  // namespace castor::tape::tapeFile
