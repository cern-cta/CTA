/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "RAOManager.hpp"
#include "common/log/LogContext.hpp"

namespace castor::tape::tapeserver::rao {

/**
 * Factory of RAOAlgorithmFactory
 * This is where the logic of instanciating the correct RAOAlgorithmFactory is implemented.
 *
 * This logic is the following:
 * If we were able to query the drive User Data Segments (UDS) limits, then it means
 * that the type of algorithm to instanciate will be an enterprise drive one (instanciate a EnterpriseRAOAlgorithmFactory)
 *
 * If we were not able to query the drive UDS limits, then it means that we will use a CTA RAO algorithm : linear, random (NonConfigurable RAO Algorithms) or
 * sltf (Configurable RAO Algorithm)
 *
 * For implementation, see the implementation of the createAlgorithmFactory() method
 */
class RAOAlgorithmFactoryFactory {
public:
  RAOAlgorithmFactoryFactory(RAOManager& raoManager, cta::log::LogContext& lc) : m_raoManager(raoManager), m_lc(lc) {}

  virtual ~RAOAlgorithmFactoryFactory() = default;

  /**
   * Returns the correct RAOAlgorithmFactory according to the informations
   * stored in the RAO manager
   */
  std::unique_ptr<RAOAlgorithmFactory> createAlgorithmFactory();

private:
  RAOManager& m_raoManager;
  cta::log::LogContext& m_lc;
};

}  // namespace castor::tape::tapeserver::rao
