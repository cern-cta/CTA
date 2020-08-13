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


#include "RAOManager.hpp"
#include "EnterpriseRAOAlgorithm.hpp"
#include "EnterpriseRAOAlgorithmFactory.hpp"
#include "NoParamRAOAlgorithmFactory.hpp"
#include "RAOAlgorithmFactoryFactory.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {
  
RAOManager::RAOManager(){
  m_config.disableRAO();
}  
  
RAOManager::RAOManager(const castor::tape::tapeserver::rao::RAOConfig & config, castor::tape::tapeserver::drive::DriveInterface * drive):m_config(config),m_drive(drive) {
}

RAOManager::RAOManager(const RAOManager& manager){
  if(this != &manager){
    m_config = manager.m_config;
    m_drive = manager.m_drive;
    m_enterpriseRaoLimits = manager.m_enterpriseRaoLimits;
    m_hasUDS = manager.m_hasUDS;
    m_maxFilesSupported = manager.m_maxFilesSupported;
  }
}

RAOManager& RAOManager::operator=(const RAOManager& manager) {
  if(this != &manager){
    m_config = manager.m_config;
    m_drive = manager.m_drive;
    m_enterpriseRaoLimits = manager.m_enterpriseRaoLimits;
    m_hasUDS = manager.m_hasUDS;
    m_maxFilesSupported = manager.m_maxFilesSupported;
  }
  return *this;
}


RAOManager::~RAOManager() {
}

bool RAOManager::useRAO() const{
  return m_config.useRAO();
}

bool RAOManager::hasUDS() const {
  return m_hasUDS;
}

bool RAOManager::isDriveEnterpriseEnabled() const {
  return m_isDriveEnterpriseEnabled;
}


castor::tape::tapeserver::drive::DriveInterface* RAOManager::getDrive() const {
  return m_drive;
}


void RAOManager::disableRAO(){
  m_config.disableRAO();
}

void RAOManager::setEnterpriseRAOUdsLimits(const SCSI::Structures::RAO::udsLimits& raoLimits) {
  m_enterpriseRaoLimits = raoLimits;
  m_maxFilesSupported = raoLimits.maxSupported;
  m_hasUDS = true;
  m_isDriveEnterpriseEnabled = true;
}

cta::optional<uint64_t> RAOManager::getMaxFilesSupported() const{
  return m_maxFilesSupported;
}

RAOConfig RAOManager::getConfig() const {
  return m_config;
}


std::vector<uint64_t> RAOManager::queryRAO(const std::vector<std::unique_ptr<cta::RetrieveJob>> & jobs, cta::log::LogContext & lc){
  RAOAlgorithmFactoryFactory raoAlgoFactoryFactory(*this,lc);
  std::unique_ptr<RAOAlgorithm> raoAlgo = raoAlgoFactoryFactory.createAlgorithmFactory()->createRAOAlgorithm();
  return raoAlgo->performRAO(jobs);
}


}}}}