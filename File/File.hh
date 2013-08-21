// ----------------------------------------------------------------------
// File: File/File.hh
// ----------------------------------------------------------------------

/************************************************************************
 * Tape Server                                                          *
 * Copyright (C) 2013 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#pragma once
#include <string>
#include "Structures.hh"

namespace Tape {
  /**
   * Class managing the reading and writing of files to and from tape.
   */
  namespace AULFile {
    /**
     * Class containing all the information related to a file being migrated to 
     * tape.
     */
    class Information {
    public:
      std::string lastKnownPath;
      uint32_t checksum;
      uint64_t nsFileId;
      uint64_t size;
      uint32_t fseq;
    };
    
    class BufferTooSmall: public Tape::Exception {
    public:
      BufferTooSmall(const std::string & what): Exception(what) {}
    };
    
    class WrongChecksum: public Tape::Exception {
    public:
      WrongChecksum(const std::string & what): Exception(what) {}
    };
    
    class WrongSize: public Tape::Exception {
    public:
      WrongSize(const std::string & what): Exception(what) {}
    };
    
    class NotReadingAFile: public Tape::Exception {
    public:
      NotReadingAFile(const std::string & what): Exception(what) {}
    };
    
    /**
     * Class keeping track of a whole tape read session over an AUL formated
     * tape. The session will keep track of the overall coherency of the session
     * and check for everything to be coherent. The tape should be mounted in
     * the drive before the AULReadSession is started (i.e. constructed).
     * Likewise, tape unmount is the business of the user.
     */
    class ReadSession{
    public:
      /**
       * Constructor of the AULReadSession. It will rewind the tape, and check the 
       * VSN value. Throws an exception in case of mismatch.
       * @param drive
       * @param VSN
       */
      ReadSession(Tape::Drive & drive, std::string VSN) throw (Tape::Exception);
      /**
       * Positions the tape for reading the file. Depending on the previous activity,
       * it is the duty of this function to determine how to best move to the next
       * file. The positioning will then be verified (header will be read). 
       * As usual, exception is thrown if anything goes wrong.
       * @param fileInfo: all relevant information passed by the stager about
       * the file.
       */
      void position(Tape::AULFile::Information fileInfo) throw (Tape::Exception);
      /**
       * After positioning at the beginning of a file for readings, this function
       * allows the reader to know which block sizes to provide.
       * If called before the end of a file read, the file reading will be 
       * interrupted and positioning to the new file will occur.
       * @return the block size in bytes.
       */
      size_t getBlockSize() throw (Tape::Exception);
      /**
       * Read data from the file. The buffer should equal to or bigger than the 
       * block size. Will try to actually fill up the provided buffer (this
       * function can trigger several read on the tape side).
       * This function will throw exceptions when problems arise (especially
       * at end of file in case of size or checksum mismatch.
       * After end of file, a new call to read without a call to position
       * will throw NotReadingAFile.
       * @param buff pointer to the data buffer
       * @param len size of the buffer
       * @return The amount of data actually copied. Zero at end of file.
       */
      size_t read(void * buff, size_t len) throw (Tape::Exception);
    };
    
    /**
     TODO
     */
    class WriteSession{
      
    };
  };
}