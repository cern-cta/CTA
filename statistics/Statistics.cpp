/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "Statistics.hpp"
#include "common/make_unique.hpp"
#include <iostream>

namespace cta { 
namespace statistics {
  
Statistics::Statistics() {
}

Statistics::Statistics(const Statistics& other) {
  if(this != &other){
    m_statisticsPerVo = other.m_statisticsPerVo;
    m_totalFiles = other.m_totalFiles;
    m_totalBytes = other.m_totalBytes;
    m_totalFilesCopyNb1 = other.m_totalFilesCopyNb1;
    m_totalBytesCopyNb1 = other.m_totalBytesCopyNb1;
    m_totalFilesCopyNbGt1 = other.m_totalFilesCopyNbGt1;
    m_totalBytesCopyNbGt1 = other.m_totalBytesCopyNbGt1;
  }
}

Statistics& Statistics::operator=(const Statistics& other) {
  if(this != &other){
    m_statisticsPerVo = other.m_statisticsPerVo;
    m_totalFiles = other.m_totalFiles;
    m_totalBytes = other.m_totalBytes;
    m_totalFilesCopyNb1 = other.m_totalFilesCopyNb1;
    m_totalBytesCopyNb1 = other.m_totalBytesCopyNb1;
    m_totalFilesCopyNbGt1 = other.m_totalFilesCopyNbGt1;
    m_totalBytesCopyNbGt1 = other.m_totalBytesCopyNbGt1;
  }
  return *this;
}

void Statistics::insertStatistics(const std::string& vo, const FileStatistics& fileStatistics){
  m_statisticsPerVo[vo] = fileStatistics;
  m_totalFiles += fileStatistics.nbMasterFiles;
  m_totalBytes += fileStatistics.masterDataInBytes;
  m_totalFilesCopyNb1 += fileStatistics.nbCopyNb1;
  m_totalBytesCopyNb1 += fileStatistics.copyNb1InBytes;
  m_totalFilesCopyNbGt1 += fileStatistics.nbCopyNbGt1;
  m_totalBytesCopyNbGt1 += fileStatistics.copyNbGt1InBytes;
}

const Statistics::StatisticsPerVo& Statistics::getAllStatistics() const {
  return m_statisticsPerVo;
}

uint64_t Statistics::getTotalFiles() const {
  return m_totalFiles;
}

uint64_t Statistics::getTotalBytes() const {
  return m_totalBytes;
}

uint64_t Statistics::getTotalFilesCopyNb1() const {
  return m_totalFilesCopyNb1;
}

uint64_t Statistics::getTotalBytesCopyNb1() const {
  return m_totalBytesCopyNb1;
}

uint64_t Statistics::getTotalFilesCopyNbGt1() const {
  return m_totalFilesCopyNbGt1;
}

uint64_t Statistics::getTotalBytesCopyNbGt1() const {
  return m_totalBytesCopyNbGt1;
}

Statistics::Builder::Builder(cta::rdbms::Rset & rset):m_rset(rset) {}

std::unique_ptr<Statistics> Statistics::Builder::build(){
  std::unique_ptr<Statistics> ret = cta::make_unique<Statistics>();
  while(m_rset.next()){
    std::string vo = m_rset.columnString("VO");
    FileStatistics fileStatistics;
    fileStatistics.nbMasterFiles = m_rset.columnUint64("TOTAL_MASTER_FILES_VO");
    fileStatistics.masterDataInBytes = m_rset.columnUint64("TOTAL_MASTER_DATA_BYTES_VO");
    fileStatistics.nbCopyNb1 = m_rset.columnUint64("TOTAL_NB_COPY_1_VO");
    fileStatistics.copyNb1InBytes = m_rset.columnUint64("TOTAL_NB_COPY_1_BYTES_VO");
    fileStatistics.nbCopyNbGt1 = m_rset.columnUint64("TOTAL_NB_COPY_NB_GT_1_VO");
    fileStatistics.copyNbGt1InBytes = m_rset.columnUint64("TOTAL_COPY_NB_GT_1_IN_BYTES_VO");
    ret->insertStatistics(vo,fileStatistics);
  }
  return ret;
}

std::ostream & operator <<(std::ostream& stream, Statistics stats) {
  stream << "{";
  stream << "\"statisticsPerVo\": [";
  for(auto & stat: stats.getAllStatistics()){
    stream << "{";
    stream << "\"vo\": \"" << stat.first << "\",";
    stream << "\"nbMasterFiles\": " << stat.second.nbMasterFiles << ",";
    stream << "\"masterDataInBytes\": " << stat.second.masterDataInBytes << ",";
    stream << "\"nbCopyNb1\": " << stat.second.nbCopyNb1 << ",";
    stream << "\"copyNb1InBytes\": " << stat.second.copyNb1InBytes << ",";
    stream << "\"nbCopyNbGt1\": " << stat.second.nbCopyNbGt1 << ",";
    stream << "\"copyNbGt1InBytes\": " << stat.second.copyNbGt1InBytes;
    stream << "},";
  }
  stream << "],"
         << "\"totalFiles\": " << stats.getTotalFiles() << ","
         << "\"totalBytes\": " << stats.getTotalBytes() << ","
         << "\"totalFilesCopyNb1\": " << stats.getTotalFilesCopyNb1() << ","
         << "\"totalBytesCopyNb1\": " << stats.getTotalBytesCopyNb1() << ","
         << "\"totalFilesCopyNbGt1\": " << stats.getTotalFilesCopyNbGt1() << ","
         << "\"totalBytesCopyNbGt1\": " << stats.getTotalBytesCopyNbGt1() << ","
         << "}";
  return stream;
}

}}