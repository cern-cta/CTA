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

#include "castor/tape/tapeserver/file/Structures.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/tape/tapegateway/FileToRecallStruct.hpp"
#include "castor/tape/tapegateway/FileToMigrateStruct.hpp"
#include "castor/tape/tapeserver/client/ClientInterface.hpp"

#include <string>

namespace castor {
  namespace tape {
    /**
     * Namespace managing the reading and writing of files to and from tape.
     */
    namespace tapeFile {
      
      enum PartOfFile
      {
         Header,
         Payload,
         Trailer
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
      
      class TapeNotEmpty: public Exception {
      public:
        TapeNotEmpty(): Exception("Trying to label a non-empty tape without the \"force\" setting") {}
      };
      
      class UnsupportedPositioningMode: public Exception {
      public:
        UnsupportedPositioningMode(): Exception("Trying to use an unsupported positioning mode") {}
      };
      
      class WrongBlockSize: public Exception {
      public:
        WrongBlockSize(): Exception("Trying to use a wrong block size") {}
      };
      
      class HeaderChecker {
      public:
        
        /**
         * Checks the hdr1
         * @param hdr1: the hdr1 header of the current file
         * @param fileInfo: the Information structure of the current file
         * @param volId: the volume id of the tape in the drive
         */
        static void checkHDR1(const HDR1 &hdr1,
        const castor::tape::tapegateway::FileToRecallStruct &filetoRecall,
        const tape::tapeserver::client::ClientInterface::VolumeInfo &volInfo) ;
        
        /**
         * Checks the uhl1
         * @param uhl1: the uhl1 header of the current file
         * @param fileInfo: the Information structure of the current file
         */
        static void checkUHL1(const UHL1 &uhl1, 
          const castor::tape::tapegateway::FileToRecallStruct &fileToRecall)
          ;
        
        /**
         * Checks the utl1
         * @param utl1: the utl1 trailer of the current file
         * @param fseq: the file sequence number of the current file
         */
        static void checkUTL1(const UTL1 &utl1, const uint32_t fseq) ;
        
        /**
         * Checks the vol1
         * @param vol1: the vol1 header of the current file
         * @param volId: the volume id of the tape in the drive
         */
        static void checkVOL1(const VOL1 &vol1, const std::string &volId) ;
        
      private:
        typedef enum {
          octal,
          decimal,
          hexadecimal
        } headerBase;
        
        /**
         * Checks the field of a header comparing it with the numerical value provided
         * @param headerField: the string of the header that we need to check
         * @param value: the unsigned numerical value against which we check
         * @param is_field_hex: set to true if the value in the header is in hexadecimal and to false otherwise
         * @param is_field_oct: set to true if the value in the header is in octal and to false otherwise
         * @return true if the header field  matches the numerical value, false otherwise
         */
        static bool checkHeaderNumericalField(const std::string &headerField,
          const uint64_t value, const headerBase base = decimal) ;
      };
      
      /**
       * Class containign static helper functions that turn block id byte
       * collections from fileToRecallStruct into uint32_t
       */
      
      class BlockId {
      public:
        static uint32_t extract(const castor::tape::tapegateway::FileToRecallStruct &);
        static void set(castor::tape::tapegateway::FileToRecallStruct &, 
          uint32_t blockId);
      }; 
      /**
       * Class keeping track of a tape label session on a tape. The session will
       * check for everything to be coherent. The tape should be mounted in
       * the drive before the LabelSession is started (i.e. constructed).
       */      
      class LabelSession {        
      public:        
        /**
         * Constructor of the LabelSession. It will rewind the tape, and label it.
         * Throws an exception in case of tape is not empty and argument force is false.
         * @param drive: drive object to which we bind the session
         * @param vid: volume name of the tape we would like to read from
         * @param force: force labeling even if tape is not empty
         */
        LabelSession(drives::DriveInterface & drive, const std::string &vid, bool force) ;
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
         * volId value. Throws an exception in case of mismatch.
         * @param drive: drive object to which we bind the session
         * @param vid: volume name of the tape we would like to read from
         */
        ReadSession(drives::DriveInterface & drive, 
                tapeserver::client::ClientInterface::VolumeInfo volInfo);
        
        /**
         * DriveGeneric object referencing the drive used during this read session
         */
        drives::DriveInterface & m_drive;
        
        /**
         * Volume Serial Number
         */
        const std::string m_vid;
        
        void setCorrupted() throw() {
          m_corrupted = true;
        }
        
        bool isCorrupted() throw() {
          return m_corrupted;
        }
        
        void lock()  {
          if(m_locked) {
            throw SessionAlreadyInUse();
          }
          if(m_corrupted) {
            throw SessionCorrupted();
          }
          m_locked = true;
        }
        
        void release() throw() {
          if(!m_locked) {
            m_corrupted = true;
          }
          m_locked = false;
        }
        
        void setCurrentFseq(uint32_t fseq) {
          m_fseq = fseq;
        }
        
        uint32_t getCurrentFseq() {
          return m_fseq;
        }       
        const tapeserver::client::ClientInterface::VolumeInfo& getVolumeInfo() {
          return m_volInfo;
        }  
        
        void setCurrentFilePart(PartOfFile currentFilePart) {
          m_currentFilePart = currentFilePart;
        }
        
        PartOfFile getCurrentFilePart() {
          return m_currentFilePart;
        }
        
      private:
                
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
        
        tapeserver::client::ClientInterface::VolumeInfo m_volInfo;
      };
      
      class ReadFile{
        
      public:
        
        /**
         * Constructor of the ReadFile. It will bind itself to an existing read session
         * and position the tape right at the beginning of the file
         * @param rs: session to be bound to
         * @param fileInfo: information about the file we would like to read
         * @param positioningMode: method used when positioning (see the PositioningMode enum)
         */
        ReadFile(ReadSession *rs, 
          const castor::tape::tapegateway::FileToRecallStruct &fileToRecall)
          ;
        
        /**
         * Destructor of the ReadFile. It will release the lock on the read session.
         */
        ~ReadFile() throw();
        
        /**
         * After positioning at the beginning of a file for readings, this function
         * allows the reader to know which block sizes to provide.
         * @return the block size in bytes.
         */
        size_t getBlockSize() ;
        
        /**
         * Reads data from the file. The buffer should be equal to or bigger than the 
         * block size.
         * @param data: pointer to the data buffer
         * @param size: size of the buffer
         * @return The amount of data actually copied. Zero at end of file.
         */
        size_t read(void *data, const size_t size) ;
      
      private:
        
        /**
         * Positions the tape for reading the file. Depending on the previous activity,
         * it is the duty of this function to determine how to best move to the next
         * file. The positioning will then be verified (header will be read). 
         * As usual, exception is thrown if anything goes wrong.
         * @param fileInfo: all relevant information passed by the stager about the file.
         */
        void position(
          const castor::tape::tapegateway::FileToRecallStruct &fileToRecall)
          ;

        /**
         * Set the block size member using the info contained within the uhl1
         * @param uhl1: the uhl1 header of the current file
         * @param fileInfo: the Information structure of the current file
         */
        void setBlockSize(const UHL1 &uhl1) ;
        
        /**
         * Block size in Bytes of the current file
         */
        size_t m_currentBlockSize;
        
        /**
         * Session to which we are attached to
         */
        ReadSession *m_session;
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
        WriteSession(drives::DriveInterface & drive, const std::string &vid, const uint32_t last_fseq, const bool compression) ;
        
        /**
         * DriveGeneric object referencing the drive used during this write session
         */
        drives::DriveInterface & m_drive;
        
        /**
         * Volume Serial Number
         */
        std::string m_vid;
        
        /**
         * set to true if the drive has compression enabled 
         */
        bool m_compressionEnabled;
        
        std::string getSiteName() throw() {
          return m_siteName;
        }
        
        std::string getHostName() throw() {
          return m_hostName;
        }
        
        void setCorrupted() throw() {
          m_corrupted = true;
        }
        
        bool isCorrupted() throw() {
          return m_corrupted;
        }
        
        void lock()  {
          if(m_locked) {
            throw SessionAlreadyInUse();
          }
          if(m_corrupted) {
            throw SessionCorrupted();
          }
          m_locked = true;
        }
        
        void release() throw() {
          if(!m_locked) {
            m_corrupted = true;
          }
          m_locked = false;
        }
        
      private:
        
        /**
         * looks for the site name in /etc/resolv.conf in the search line and saves the upper-cased value in siteName
         */
        void setSiteName() ;
        
        /**
         * uses gethostname to get the upper-cased hostname without the domain name
         */
        void setHostName() ;
        
        /**
         * The following two variables are needed when writing the headers and trailers, sitename is grabbed from /etc/resolv.conf
         */
        std::string m_siteName;
        
        /**
         * hostname is instead gotten from gethostname()
         */
        std::string m_hostName;
        
        /**
         * set to true in case the write operations do (or try to do) something illegal
         */
        bool m_corrupted;
        
        /**
         * Session lock to be sure that a read session is owned by maximum one WriteFile object 
         */
        bool m_locked;
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
        WriteFile(WriteSession *ws, 
          const castor::tape::tapegateway::FileToMigrateStruct & fileToMigrate,
          const size_t blockSize) ;
        
        /**
         * Returns the block id of the current position
         * @return blockId of current position
         */
        uint32_t getPosition() ;
        
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
        void write(const void *data, const size_t size) ;
        
        /**
         * Closes the file by writing the corresponding trailer on tape
         * HAS TO BE CALL EXPLICITLY
         */
        void close() ;
        
        /**
         * Destructor of the WriteFile object. Releases the WriteSession
         */
        ~WriteFile() throw();
        
      private:
        
        /**
         * Block size in Bytes of the current file
         */
        size_t m_currentBlockSize;
        
        /**
         * Session to which we are attached to
         */
        WriteSession *m_session;
        
        /**
         * Information that we have about the current file to be written and that
         * will be used to write appropriate headers and trailers
         */
        const castor::tape::tapegateway::FileToMigrateStruct & m_fileToMigrate;
        
        /**
         * set to true whenever the constructor is called and to false when close() is called         
         */
        bool m_open;
        
        /**
         * set to false initially, set to true after at least one successful nonzero writeBlock operation         
         */
        bool m_nonzeroFileWritten;
        
        /**
         * number of blocks written for the current file         
         */
        int m_numberOfBlocks;
      };
    }
    
    /**
     * Namespace managing the reading and writing of files to and from disk.
     */
    namespace diskFile {
      
      class ReadFile {
      public:
        
        /**
         * Constructor of the ReadFile class. It opens the file for reading with the O_RDONLY flag.
         * @param url: Uniform Resource Locator of the file we want to read
         */
        ReadFile(const std::string &url) ;
        
        /**
         * Reads data from the file.
         * @param data: pointer to the data buffer
         * @param size: size of the buffer
         * @return The amount of data actually copied. Zero at end of file.
         */
        size_t read(void *data, const size_t size) ;
        
        /**
         * Destructor of the ReadFile class. It closes the corresponding file descriptor.
         */
        ~ReadFile() throw();
        
      private:
        int m_fd;
      };
      
      class WriteFile {
      public:
        
        /**
         * Constructor of the WriteFile class. It opens the file for writing with the O_WRONLY, O_CREAT and O_EXCL flags.
         * @param url: Uniform Resource Locator of the file we want to write
         */
        WriteFile(const std::string &url) ;
        
        /**
         * Writes a block of data on disk
         * @param data: buffer to copy the data from
         * @param size: size of the buffer
         */
        void write(const void *data, const size_t size) ;
        
        /**
         * Closes the corresponding file descriptor, which may throw an exception.
         */
        void close() ;
        
        /**
         * Destructor of the WriteFile class.
         */
        ~WriteFile() throw();
        
      private:
        int m_fd;
        bool closeTried;
      };
    } //end of namespace diskFile
  } //end of namespace tape
} //end of namespace castor
