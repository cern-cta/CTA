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

#include "tapeserver/castor/tape/tapeserver/file/Structures.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/VolumeInfo.hpp"
#include "common/exception/Exception.hpp"
#include "scheduler/ArchiveJob.hpp"
#include "scheduler/RetrieveJob.hpp"

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
         HeaderProcessing,
         Payload,
         Trailer
      };

      class TapeFormatError: public cta::exception::Exception {
      public:
        TapeFormatError(const std::string & what): Exception(what) {}
      };
      
      class TapeMediaError: public cta::exception::Exception {
      public:
        TapeMediaError(const std::string & what): Exception(what) {}
      };
      
      class EndOfFile: public cta::exception::Exception {
      public:
        EndOfFile(): Exception("End Of File reached") {}
      };
      
      class SessionAlreadyInUse: public cta::exception::Exception {
      public:
        SessionAlreadyInUse(): Exception("Session already in use") {}
      };
      
      class SessionCorrupted: public cta::exception::Exception {
      public:
        SessionCorrupted(): Exception("Session corrupted") {}
      };
      
      class FileClosedTwice: public cta::exception::Exception {
      public:
        FileClosedTwice(): Exception("Trying to close a file twice") {}
      };
      
      class ZeroFileWritten: public cta::exception::Exception {
      public:
        ZeroFileWritten(): Exception("Trying to write a file with size 0") {}
      };
      
      class TapeNotEmpty: public cta::exception::Exception {
      public:
        TapeNotEmpty(): Exception("Trying to label a non-empty tape without the \"force\" setting") {}
      };
      
      class UnsupportedPositioningMode: public cta::exception::Exception {
      public:
        UnsupportedPositioningMode(): Exception("Trying to use an unsupported positioning mode") {}
      };
      
      class WrongBlockSize: public cta::exception::Exception {
      public:
        WrongBlockSize(): Exception("Trying to use a wrong block size") {}
      };
      
      class HeaderChecker {
      public:
        
        /**
         * Checks the hdr1
         * @param hdr1: the hdr1 header of the current file
         * @param filetoRecall: the Information structure of the current file
         * @param volId: the volume id of the tape in the drive
         */
        static void checkHDR1(const HDR1 &hdr1,
        const cta::RetrieveJob &filetoRecall,
        const tape::tapeserver::daemon::VolumeInfo &volInfo) ;
        
        /**
         * Checks the uhl1
         * @param uhl1: the uhl1 header of the current file
         * @param fileToRecall: the Information structure of the current file
         */
        static void checkUHL1(const UHL1 &uhl1, 
          const cta::RetrieveJob &fileToRecall)
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
       * Class that labels a tape.  This class assumes the tape to be labeled is
       * already mounted and rewound.
       */      
      class LabelSession {        
      public:        
        /**
         * Constructor of the LabelSession. This constructor will label the
         * tape in the specified drive.  This constructor assumes the tape to be
         * labeled is already mounted and rewound.
         * @param drive: drive object to which we bind the session
         * @param vid: volume name of the tape we would like to read from
         * @param lbp The boolean variable for logical block protection mode.
         *            If it is true than the label will be written with LBP or 
         *            without otherwise.
         *
         */
        LabelSession(tapeserver::drive::DriveInterface & drive, 
          const std::string &vid, const bool lbp);
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
         * @param useLbp: castor.conf option to use or not to use LBP in tapeserverd
         */
        ReadSession(tapeserver::drive::DriveInterface & drive, 
                tapeserver::daemon::VolumeInfo volInfo,
                const bool useLbp);
        
        /**
         * DriveGeneric object referencing the drive used during this read session
         */
        tapeserver::drive::DriveInterface & m_drive;
        
        /**
         * Volume Serial Number
         */
        const std::string m_vid;
        
        /**
        * The boolean variable describing to use on not to use Logical
        * Block Protection.
        */
        const bool m_useLbp;

        void setCorrupted() throw() {
          m_corrupted = true;
        }
        
        bool isCorrupted() throw() {
          return m_corrupted;
        }
        
        bool isTapeWithLbp() throw() {
          return m_detectedLbp;
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
        const tapeserver::daemon::VolumeInfo& getVolumeInfo() {
          return m_volInfo;
        }  
        
        void setCurrentFilePart(PartOfFile currentFilePart) {
          m_currentFilePart = currentFilePart;
        }
        
        PartOfFile getCurrentFilePart() {
          return m_currentFilePart;
        }
        
        std::string getLBPMode() {
          if (m_useLbp && m_detectedLbp)
            return "LBP_On";
          else if (!m_useLbp && m_detectedLbp)
            return "LBP_Off_but_present";
          else if (!m_detectedLbp)
            return "LBP_Off";
          throw cta::exception::Exception("In ReadSession::getLBPMode(): unexpected state");
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
        
        const tapeserver::daemon::VolumeInfo m_volInfo;

        /**
        * The boolean variable indicates that the tape has VOL1 with enabled LBP
        */
        bool m_detectedLbp;
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
          const cta::RetrieveJob &fileToRecall)
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
        
        /**
         * Returns the LBP access mode.
         * @return The LBP mode.
         */
        std::string getLBPMode();
      
      private:
        void positionByFseq(const cta::RetrieveJob &fileToRecall) ;
        void positionByBlockID(const cta::RetrieveJob &fileToRecall) ;
        /**
         * Positions the tape for reading the file. Depending on the previous activity,
         * it is the duty of this function to determine how to best move to the next
         * file. The positioning will then be verified (header will be read). 
         * As usual, exception is thrown if anything goes wrong.
         * @param fileInfo: all relevant information passed by the stager about the file.
         */
        void position(
          const cta::RetrieveJob &fileToRecall)
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
        
        /**
         * What kind of command we use to position ourself on the tape (fseq or blockid)
         */
        cta::PositioningMethod m_positionCommandCode;

        /**
         * Description of the LBP mode with which the files is read.
         */
        std::string m_LBPMode;

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
         * @param useLbp: castor.conf option to use or not to use LBP in tapeserverd
         */
        WriteSession(tapeserver::drive::DriveInterface & drive, 
                const tapeserver::daemon::VolumeInfo& volInfo, 
                const uint32_t last_fseq, const bool compression,
                const bool useLbp) ;
        
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
        
        bool isTapeWithLbp() throw() {
          return m_detectedLbp;
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
        
        void release() {
          if(!m_locked) {
            m_corrupted = true;
            throw SessionCorrupted();
          }
          m_locked = false;
        }
        
        const tapeserver::daemon::VolumeInfo&
        getVolumeInfo()  const {
          return m_volInfo;
        }
        
        /**
         * Checks that a fSeq we are intending to write the the proper one,
         * following the previous written one in sequence.
         * This is to be used by the tapeWriteTask right before writing the file
         * @param nextFSeq The fSeq we are about to write.
         */
        void validateNextFSeq (int nextFSeq) const {
          if (nextFSeq != m_lastWrittenFSeq + 1) {
            cta::exception::Exception e;
            e.getMessage() << "In WriteSession::validateNextFSeq: wrong fSeq sequence: lastWrittenFSeq="
              << m_lastWrittenFSeq << " nextFSeq=" << nextFSeq;
            throw e;
          }
        }
        
        /**
         * Checks the value of the lastfSeq written to tape and records it.
         * This is to be used by the tape write task right after closing the
         * file.
         * @param writtenFSeq the fSeq of the file
         */
        void reportWrittenFSeq (int writtenFSeq) {
          if (writtenFSeq != m_lastWrittenFSeq + 1) {
            cta::exception::Exception e;
            e.getMessage() << "In WriteSession::reportWrittenFSeq: wrong fSeq reported: lastWrittenFSeq="
              << m_lastWrittenFSeq << " writtenFSeq=" << writtenFSeq;
            throw e;
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
         * keep track of the fSeq we are writing to tape
         */
        int m_lastWrittenFSeq; 
        
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
          const cta::ArchiveJob & fileToMigrate,
          const size_t blockSize) ;
        
        /**
         * Returns the block id of the current position
         * @return blockId of current position
         */
        uint32_t getPosition() ;
        
        /**
         * Retuns the block is of the first block of the header of the file.
         * This is changed from 1 to 0 for the first file on the tape (fseq=1)
         * @return blockId of the first tape block of the file's header.
         */
        uint32_t getBlockId() ;
        
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
        WriteSession *m_session;
        
        /**
         * Information that we have about the current file to be written and that
         * will be used to write appropriate headers and trailers
         */
        const cta::ArchiveJob & m_fileToMigrate;
        
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
    }
  } //end of namespace tape
} //end of namespace castor
