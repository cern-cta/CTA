/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "Statistics.hpp"

namespace cta::statistics {

void Statistics::insertPerVOStatistics(const std::string& vo, const FileStatistics& fileStatistics) {
  m_statisticsPerVo[vo] = fileStatistics;
  m_totalFiles += fileStatistics.nbMasterFiles;
  m_totalBytes += fileStatistics.masterDataInBytes;
  m_totalFilesCopyNb1 += fileStatistics.nbCopyNb1;
  m_totalBytesCopyNb1 += fileStatistics.copyNb1InBytes;
  m_totalFilesCopyNbGt1 += fileStatistics.nbCopyNbGt1;
  m_totalBytesCopyNbGt1 += fileStatistics.copyNbGt1InBytes;
}

const Statistics::StatisticsPerVo& Statistics::getAllVOStatistics() const {
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

time_t Statistics::getUpdateTime() const {
  return m_updateTime;
}

std::unique_ptr<Statistics> Statistics::Builder::build(cta::rdbms::Rset* rset) {
  std::unique_ptr<Statistics> ret = std::make_unique<Statistics>();
  while (rset->next()) {
    // loop over each VO
    std::string vo = rset->columnString("VO");
    FileStatistics fileStatistics;
    fileStatistics.nbMasterFiles = rset->columnUint64("TOTAL_MASTER_FILES_VO");
    fileStatistics.masterDataInBytes = rset->columnUint64("TOTAL_MASTER_DATA_BYTES_VO");
    fileStatistics.nbCopyNb1 = rset->columnUint64("TOTAL_NB_COPY_1_VO");
    fileStatistics.copyNb1InBytes = rset->columnUint64("TOTAL_NB_COPY_1_BYTES_VO");
    fileStatistics.nbCopyNbGt1 = rset->columnUint64("TOTAL_NB_COPY_NB_GT_1_VO");
    fileStatistics.copyNbGt1InBytes = rset->columnUint64("TOTAL_COPY_NB_GT_1_IN_BYTES_VO");
    // insert the perVO file statistics
    ret->insertPerVOStatistics(vo, fileStatistics);
  }
  // Set the statistics update time to now
  ret->m_updateTime = time(nullptr);
  return ret;
}

std::ostream & operator <<(std::ostream& stream, const Statistics& stats) {
  stream << "{"
         << "\"statisticsPerVo\": [";
  auto allVoStatistics = stats.getAllVOStatistics();
  uint64_t nbElementsVoStatistics = allVoStatistics.size();
  uint64_t i = 0;
  for (auto & stat : allVoStatistics) {
    stream << "{"
           << "\"vo\": \"" << stat.first << "\","
           << "\"nbMasterFiles\": " << stat.second.nbMasterFiles << ","
           << "\"masterDataInBytes\": " << stat.second.masterDataInBytes << ","
           << "\"nbCopyNb1\": " << stat.second.nbCopyNb1 << ","
           << "\"copyNb1InBytes\": " << stat.second.copyNb1InBytes << ","
           << "\"nbCopyNbGt1\": " << stat.second.nbCopyNbGt1 << ","
           << "\"copyNbGt1InBytes\": " << stat.second.copyNbGt1InBytes
           << "}";
    if (++i < nbElementsVoStatistics) {
      stream << ",";
    }
  }
  stream << "],"
         << "\"totalFiles\": " << stats.getTotalFiles() << ","
         << "\"totalBytes\": " << stats.getTotalBytes() << ","
         << "\"totalFilesCopyNb1\": " << stats.getTotalFilesCopyNb1() << ","
         << "\"totalBytesCopyNb1\": " << stats.getTotalBytesCopyNb1() << ","
         << "\"totalFilesCopyNbGt1\": " << stats.getTotalFilesCopyNbGt1() << ","
         << "\"totalBytesCopyNbGt1\": " << stats.getTotalBytesCopyNbGt1() << ","
         << "\"updateTime\": " << stats.getUpdateTime()
         << "}";
  return stream;
}

} // namespace cta::statistics
