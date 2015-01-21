/******************************************************************************
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

#include "castor/tape/tapeserver/drive/FakeDrive.hpp"

namespace {
  const long unsigned int max_fake_drive_record_length = 1000;
  const char filemark[] = "";
}

castor::tape::tapeserver::drive::FakeDrive::FakeDrive(uint64_t capacity,
    FailureMoment failureMoment, bool failToMount) throw(): 
  m_currentPosition(0), m_tapeCapacity(capacity), m_beginOfCompressStats(0),
  m_failureMoment(failureMoment), m_tapeOverflow(false), m_failToMount(failToMount)
{
  m_tape.reserve(max_fake_drive_record_length);
}

castor::tape::tapeserver::drive::FakeDrive::FakeDrive(bool failToMount) throw(): 
  m_currentPosition(0), m_tapeCapacity(std::numeric_limits<uint64_t>::max()), m_beginOfCompressStats(0),
  m_failureMoment(OnWrite), m_tapeOverflow(false), m_failToMount(failToMount)
{
  m_tape.reserve(max_fake_drive_record_length);
}

castor::tape::tapeserver::drive::compressionStats castor::tape::tapeserver::drive::FakeDrive::getCompression()  {
  castor::tape::tapeserver::drive::compressionStats stats;
  for(unsigned int i=m_beginOfCompressStats;i<m_tape.size();++i){
    stats.toTape += m_tape[i].data.length();
  }
  
  //that way we set also stats for reading
  stats.fromTape  = stats.toTape;
  return stats;
}
void castor::tape::tapeserver::drive::FakeDrive::clearCompressionStats()  {
  m_beginOfCompressStats=m_tape.size();
}
castor::tape::tapeserver::drive::deviceInfo castor::tape::tapeserver::drive::FakeDrive::getDeviceInfo()  {
  deviceInfo devInfo;
  devInfo.product = "Fake Drv";
  devInfo.productRevisionLevel = "0.1";
  devInfo.vendor = "ACME Ind";
  devInfo.serialNumber = "123456";
  return devInfo;
}
std::string castor::tape::tapeserver::drive::FakeDrive::getSerialNumber()  {
  throw castor::exception::Exception("FakeDrive::getSerialNumber Not implemented");
}
void castor::tape::tapeserver::drive::FakeDrive::positionToLogicalObject(uint32_t blockId)  {
  m_currentPosition = blockId;
}
castor::tape::tapeserver::drive::positionInfo castor::tape::tapeserver::drive::FakeDrive::getPositionInfo()  {
  positionInfo pos;
  pos.currentPosition = m_currentPosition;
  pos.dirtyBytesCount = 0;
  pos.dirtyObjectsCount = 0;
  pos.oldestDirtyObject = 0;
  return pos;
}

std::vector<uint16_t> castor::tape::tapeserver::drive::FakeDrive::getTapeAlertCodes() {
  std::vector<uint16_t> empty;
  return empty;
}

std::vector<std::string> castor::tape::tapeserver::drive::FakeDrive::getTapeAlerts(
  const std::vector<uint16_t>&)  {
  std::vector<std::string> empty;
  return empty;
}

std::vector<std::string> castor::tape::tapeserver::drive::FakeDrive::getTapeAlertsCompact(
  const std::vector<uint16_t>&)  {
  std::vector<std::string> empty;
  return empty;
}
void castor::tape::tapeserver::drive::FakeDrive::setDensityAndCompression(bool compression, unsigned char densityCode)  {
  throw castor::exception::Exception("FakeDrive::setDensityAndCompression Not implemented");
}
castor::tape::tapeserver::drive::driveStatus castor::tape::tapeserver::drive::FakeDrive::getDriveStatus()  {
  throw castor::exception::Exception("FakeDrive::getDriveStatus Not implemented");
}
castor::tape::tapeserver::drive::tapeError castor::tape::tapeserver::drive::FakeDrive::getTapeError()  {
  throw castor::exception::Exception("FakeDrive::getTapeError Not implemented");
}
void castor::tape::tapeserver::drive::FakeDrive::setSTBufferWrite(bool bufWrite)  {
  throw castor::exception::Exception("FakeDrive::setSTBufferWrite Not implemented");
}
void castor::tape::tapeserver::drive::FakeDrive::fastSpaceToEOM(void)  {
  m_currentPosition = m_tape.size()-1;
}
void castor::tape::tapeserver::drive::FakeDrive::rewind(void)  {
  m_currentPosition = 0;
}
void castor::tape::tapeserver::drive::FakeDrive::spaceToEOM(void)  {
  m_currentPosition = m_tape.size()-1;
}
void castor::tape::tapeserver::drive::FakeDrive::spaceFileMarksBackwards(size_t count)  {
  if(!count) return;
  size_t countdown = count;
  std::vector<std::string>::size_type i=0;
  for(i=m_currentPosition; i!=(std::vector<std::string>::size_type)-1 and countdown!=0; i--) {
    if(!m_tape[i].data.compare(filemark)) countdown--;
  }
  if(countdown) {
    throw castor::exception::Exception("FakeDrive::spaceFileMarksBackwards");
  }  
  m_currentPosition = i-1; //BOT side of the filemark
}
void castor::tape::tapeserver::drive::FakeDrive::spaceFileMarksForward(size_t count)  {
  if(!count) return;
  size_t countdown = count;
  std::vector<std::string>::size_type i=0;
  for(i=m_currentPosition; i!=m_tape.size() and countdown!=0; i++) {
    if(!m_tape[i].data.compare(filemark)) countdown--;
  }
  if(countdown) {
    throw castor::exception::Errnum(EIO, "Failed FakeDrive::spaceFileMarksForward");
  }
  m_currentPosition = i; //EOT side of the filemark
}
void castor::tape::tapeserver::drive::FakeDrive::unloadTape(void)  {
}
void castor::tape::tapeserver::drive::FakeDrive::flush(void)  {
  if (m_failureMoment == OnFlush) {
    if (m_tapeOverflow) {
      throw castor::exception::Errnum(ENOSPC, "Error in castor::tape::tapeserver::drive::FakeDrive::flush");
    }
  }
}

uint64_t castor::tape::tapeserver::drive::FakeDrive::getRemaingSpace(uint32_t currentPosition)
{
  if (!currentPosition)
    return m_tapeCapacity;
  return m_tape[currentPosition-1].remainingSpaceAfter;
}

void castor::tape::tapeserver::drive::FakeDrive::writeSyncFileMarks(size_t count)  {
  if(count==0) return;
  m_tape.resize(m_currentPosition+count);
  for(size_t i=0; i<count; ++i) {
    m_tape[m_currentPosition].data = filemark;
    // We consider the file mark takes "no space"
    m_tape[m_currentPosition].remainingSpaceAfter = getRemaingSpace(m_currentPosition);
    m_currentPosition++;
  }
}
void castor::tape::tapeserver::drive::FakeDrive::writeImmediateFileMarks(size_t count)  {
  writeSyncFileMarks(count);
}
void castor::tape::tapeserver::drive::FakeDrive::writeBlock(const void * data, size_t count)  {
  // check that the next block will fit in the remaining space on the tape
  // and compute what will be left after
  uint64_t remainingSpaceAfterBlock;
  if (count > getRemaingSpace(m_currentPosition)) {
    if (m_failureMoment == OnWrite) {
      throw castor::exception::Errnum(ENOSPC, "Error in castor::tape::tapeserver::drive::FakeDrive::writeBlock");
    } else {
      remainingSpaceAfterBlock = 0;
      m_tapeOverflow = true;
    }
  } else {
    remainingSpaceAfterBlock = getRemaingSpace(m_currentPosition) - count;
  }
  m_tape.resize(m_currentPosition+1);
  m_tape[m_currentPosition].data.assign((const char *)data, count);
  m_tape[m_currentPosition].remainingSpaceAfter = remainingSpaceAfterBlock;
  m_currentPosition++;
}
ssize_t castor::tape::tapeserver::drive::FakeDrive::readBlock(void *data, size_t count)  {
  if(count < m_tape[m_currentPosition].data.size()) {
    throw castor::exception::Exception("Block size too small in FakeDrive::readBlock");
  }
  size_t bytes_copied = m_tape[m_currentPosition].data.copy((char *)data, m_tape[m_currentPosition].data.size());
  m_currentPosition++;
  return bytes_copied;
}
std::string castor::tape::tapeserver::drive::FakeDrive::contentToString() throw() {
  std::stringstream exc;
  exc << std::endl;
  exc << "Tape position: " << m_currentPosition << std::endl;
  exc << std::endl;
  exc << "Tape contents:" << std::endl;
  for(unsigned int i=0; i<m_tape.size(); i++) {
    exc << i << ": " << m_tape[i].data << std::endl;
  }
  exc << std::endl;
  return exc.str();
}
void castor::tape::tapeserver::drive::FakeDrive::readExactBlock(void *data, size_t count, std::string context)  {
  if(count != m_tape[m_currentPosition].data.size()) {
    std::stringstream exc;
    exc << "Wrong block size in FakeDrive::readExactBlock. Expected: " << count 
        << " Found: " << m_tape[m_currentPosition].data.size() 
        << " Position: " << m_currentPosition 
        << " String: " << m_tape[m_currentPosition].data << std::endl;
    exc << contentToString();
    throw castor::exception::Exception(exc.str());
  }
  if(count != m_tape[m_currentPosition].data.copy((char *)data, count)) {
    throw castor::exception::Exception("Failed FakeDrive::readExactBlock");
  }
  m_currentPosition++;
}
void castor::tape::tapeserver::drive::FakeDrive::readFileMark(std::string context)  {
  if(m_tape[m_currentPosition].data.compare(filemark)) {
    throw castor::exception::Exception("Failed FakeDrive::readFileMark");
  }
  m_currentPosition++;  
}
bool castor::tape::tapeserver::drive::FakeDrive::waitUntilReady(int timeoutSecond)  {
  if (m_failToMount)
    throw castor::exception::Exception("In FakeDrive::waitUntilReady: Failed to mount the tape");
  return true;
}  
bool castor::tape::tapeserver::drive::FakeDrive::isWriteProtected()  {
  return false;
}
bool castor::tape::tapeserver::drive::FakeDrive::isAtBOT()  {
  return m_currentPosition==0;
}
bool castor::tape::tapeserver::drive::FakeDrive::isAtEOD()  {
  return m_currentPosition==m_tape.size()-1;
}

bool castor::tape::tapeserver::drive::FakeDrive::isTapeBlank() {
  return m_tape.empty();
}

bool castor::tape::tapeserver::drive::FakeDrive::hasTapeInPlace() {
  return true;
}
