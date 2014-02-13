/******************************************************************************
 *                      File.hpp
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
#include <string>
#include "Structures.hpp"
#include "../exception/Exception.hpp"

namespace castor {
  namespace tape {
    /**
     * Class managing the reading and writing of files to and from tape.
     */
    namespace AULFile {
      /**
       * Class containing all the information related to a file being migrated to 
       * tape.
       */
      class Information { //no information about path and filename here as it cannot be used nor checked on tape
      public:
        uint32_t checksum;
        uint64_t nsFileId;
        uint64_t size;
        uint32_t fseq; //this is the payload (i.e. real file) sequence number, not the tape file sequence number (which would include headers and trailers as well)
        uint32_t blockId;
      };
      
      class TapeFormatError: public Exception {
      public:
        TapeFormatError(const std::string & what): Exception(what) {}
      };
      
      class TapeMediaError: public Exception {
      public:
        TapeMediaError(const std::string & what): Exception(what) {}
      };
      
      class EndOfFile: public Exception {
      public:
        EndOfFile(): Exception("End Of File reached") {}
      };
      
      class SessionAlreadyInUse: public Exception {
      public:
        SessionAlreadyInUse(): Exception("Session already in use") {}
      };
      
      class SessionCorrupted: public Exception {
      public:
        SessionCorrupted(): Exception("Session corrupted") {}
      };
      
      class FileClosedTwice: public Exception {
      public:
        FileClosedTwice(): Exception("Trying to close a file twice") {}
      };
      
      class ZeroFileWritten: public Exception {
      public:
        ZeroFileWritten(): Exception("Trying to write a file with size 0") {}
      };
      
      class HeaderChecker {
      public:
        
        /**
         * Checks the field of a header comparing it with the numerical value provided
         * @param hdr1: the hdr1 header of the current file
         * @param fileInfo: the Information structure of the current file
         */
        static void checkHDR1(const HDR1 &hdr1, const Information &fileInfo, const std::string &volId) throw (Exception);
        
        /**
         * Checks the uhl1
         * @param uhl1: the uhl1 header of the current file
         * @param fileInfo: the Information structure of the current file
         */
        static void checkUHL1(const UHL1 &uhl1, const Information &fileInfo) throw (Exception);
        
        /**
         * Checks the utl1
         * @param utl1: the utl1 trailer of the current file
         * @param fseq: the file sequence number of the current file
         */
        static void checkUTL1(const UTL1 &utl1, const uint32_t fseq) throw (Exception);
        
        /**
         * checks the volume label to make sure the label is valid and that we
         * have the correct tape (checks VSN). Leaves the tape at the end of the
         * first header block (i.e. right before the first data block) in case
         * of success, or rewinds the tape in case of volume label problems.
         * Might also leave the tape in unknown state in case any of the st
         * operations fail.
         */
        static void checkVOL1(const VOL1 &vol1, const std::string &volId) throw (Exception);
        
      private:
        
        /**
         * Checks the field of a header comparing it with the numerical value provided
         * @param headerField: the string of the header that we need to check
         * @param value: the unsigned numerical value against which we check
         * @param is_field_hex: set to true if the value in the header is in hexadecimal and to false otherwise
         * @param is_field_oct: set to true if the value in the header is in octal and to false otherwise
         * @return true if the header field  matches the numerical value, false otherwise
         */
        static bool checkHeaderNumericalField(const std::string &headerField, const uint64_t &value, bool is_field_hex=false, bool is_field_oct=false) throw (Exception);
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
        
        /**
         * Constructor of the ReadSession. It will rewind the tape, and check the 
         * VSN value. Throws an exception in case of mismatch.
         * @param drive: drive object to which we bind the session
         * @param volId: volume name of the tape we would like to read from
         */
        ReadSession(drives::DriveGeneric & drive, std::string volId) throw (Exception);
        
        /**
         * DriveGeneric object referencing the drive used during this read session
         */
        drives::DriveGeneric & dg;
        
        /**
         * Volume Serial Number
         */
        std::string VSN;
        
        /**
         * Session lock to be sure that a read session is owned by maximum one ReadFile object 
         */
        bool lock;
        
        void setCorrupted() {
          corrupted = true;
        }
        
        bool isCorrupted() {
          return corrupted;
        }
        
      private:
                
        /**
         * set to true in case the destructor of ReadFile finds a missing lock on its session
         */
        bool corrupted;
      };
      
      class ReadFile{
        
      public:
        
        /**
         * Constructor of the ReadFile. It will bind itself to an existing read session
         * and position the tape right at the beginning of the file
         * @param rs: session to be bound to
         * @param fileInfo: information about the file we would like to read
         */
        ReadFile(ReadSession *rs, const Information &fileInfo) throw (Exception);
        
        /**
         * Destructor of the ReadFile. It will release the lock on the read session.
         */
        ~ReadFile() throw (Exception);
        
        /**
         * After positioning at the beginning of a file for readings, this function
         * allows the reader to know which block sizes to provide.
         * @return the block size in bytes.
         */
        size_t getBlockSize() throw (Exception);
        
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
        size_t read(void * buff, size_t len) throw (Exception);
      
      private:
        
        /**
         * Positions the tape for reading the file. Depending on the previous activity,
         * it is the duty of this function to determine how to best move to the next
         * file. The positioning will then be verified (header will be read). 
         * As usual, exception is thrown if anything goes wrong.
         * @param fileInfo: all relevant information passed by the stager about
         * the file.
         */
        void position(const Information &fileInfo) throw (Exception);

        /**
         * Set the block size member using the info contained within the uhl1
         * @param uhl1: the uhl1 header of the current file
         * @param fileInfo: the Information structure of the current file
         */
        void setBlockSize(const UHL1 &uhl1) throw (Exception);
        
        /**
         * Block size in Bytes of the current file
         */
        size_t current_block_size;
        
        /**
         * Session to which we are attached to
         */
        ReadSession *session;
      };

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
         */
        WriteSession(drives::DriveGeneric & drive, std::string volId, uint32_t last_fseq, bool compression) throw (Exception);
        
        /**
         * DriveGeneric object referencing the drive used during this write session
         */
        drives::DriveGeneric & dg;
        
        /**
         * Volume Serial Number
         */
        std::string VSN;
        
        /**
         * Session lock to be sure that a write session is owned by maximum one WriteFile object 
         */
        bool lock;
        
        /**
         * set to true if the drive has compression enabled 
         */
        bool compressionEnabled;
        
        std::string getSiteName() throw() {
          return siteName;
        }
        
        std::string getHostName() throw() {
          return hostName;
        }
        
        void setCorrupted() {
          corrupted = true;
        }
        
        bool isCorrupted() {
          return corrupted;
        }
        
      private:
        
        /**
         * checks the volume label to make sure the label is valid and that we
         * have the correct tape (checks VSN). Leaves the tape at the end of the
         * first header block (i.e. right before the first data block) in case
         * of success, or rewinds the tape in case of volume label problems.
         * Might also leave the tape in unknown state in case any of the st
         * operations fail.
         */
        void checkVOL1() throw (Exception);
        
        /**
         * looks for the site name in /etc/resolv.conf in the search line and saves the upper-cased value in siteName
         */
        void setSiteName() throw (Exception);
        
        /**
         * uses gethostname to get the upper-cased hostname without the domain name
         */
        void setHostName() throw (Exception);
        
        /**
         * The following two variables are needed when writing the headers and trailers, sitename is grabbed from /etc/resolv.conf
         */
        std::string siteName;
        
        /**
         * hostname is instead gotten from gethostname()
         */
        std::string hostName;
        
        /**
         * set to true in case the write operations do (or try to do) something illegal
         */
        bool corrupted;
      };
      
      class WriteFile {        
      public:
        
        /**
         * Constructor of the WriteFile. It will bind itself to an existing write session
         * and position the tape right at the end of the last file
         * @param ws: session to be bound to
         * @param fileInfo: information about the file we want to read
         * @param blockSize: size of blocks we want to use in writing
         */
        WriteFile(WriteSession *ws, Information fileinfo, size_t blockSize) throw (Exception);
        
        /**
         * Returns the block id of the current position
         * @return blockId of current position
         */
        uint32_t getPosition() throw (Exception);
        
        /**
         * Writes a block of data on tape
         * @param data: buffer to copy the data from
         * @param size: size of the buffer
         */
        void write(const void *data, size_t size) throw (Exception);
        
        /**
         * Closes the file by writing the corresponding trailer on tape
         */
        void close() throw (Exception);
        
        /**
         * Destructor of the WriteFile object. Releases the WriteSession
         */
        ~WriteFile() throw (Exception);
        
      private:
        
        /**
         * Block size in Bytes of the current file
         */
        size_t current_block_size;
        
        /**
         * Session to which we are attached to
         */
        WriteSession *session;
        
        /**
         * Information that we have about the current file to be written and that
         * will be used to write appropriate headers and trailers
         */
        Information fileinfo;
        
        /**
         * set to true whenever the constructor is called and to false when close() is called         
         */
        bool open;
        
        /**
         * set to false initially, set to true after at least one successful nonzero writeBlock operation         
         */
        bool nonzero_file_written;
        
        /**
         * number of blocks written for the current file         
         */
        int number_of_blocks;
      };
    };
  }
}
