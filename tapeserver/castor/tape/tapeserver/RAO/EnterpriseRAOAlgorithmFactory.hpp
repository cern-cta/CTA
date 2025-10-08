/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "RAOAlgorithmFactory.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"

namespace castor::tape::tapeserver::rao {

/**
 * Factory of EnterpriseRAOAlgorithm.
 */
class EnterpriseRAOAlgorithmFactory : public RAOAlgorithmFactory{
public:
  /**
   * Constructor of this factory
   * @param drive the DriveInterface to perform a RAO query
   * @param maxFilesSupported the maximum number of files the drive supports to perform a RAO query
   */
  EnterpriseRAOAlgorithmFactory(drive::DriveInterface* drive, const uint64_t maxFilesSupported) :
    m_drive(drive), m_maxFilesSupported(maxFilesSupported) { }

  ~EnterpriseRAOAlgorithmFactory() final = default;

  /**
   * Returns an Enteprise RAO Algorithm
   */
  std::unique_ptr<RAOAlgorithm> createRAOAlgorithm() override;

private:
  drive::DriveInterface * m_drive;
  uint64_t m_maxFilesSupported;
};

} // namespace castor::tape::tapeserver::rao
