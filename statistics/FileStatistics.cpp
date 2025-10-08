/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */


#include "FileStatistics.hpp"

namespace cta::statistics {

FileStatistics& FileStatistics::operator+=(const FileStatistics& other) {
  nbMasterFiles += other.nbMasterFiles;
  masterDataInBytes += other.masterDataInBytes;
  nbCopyNb1 += other.nbCopyNb1;
  copyNb1InBytes += other.copyNb1InBytes;
  nbCopyNbGt1 += other.nbCopyNbGt1;
  copyNbGt1InBytes += other.copyNbGt1InBytes;
  return *this;
}

} // namespace cta::statistics
