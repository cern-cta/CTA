/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
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

#pragma once

#include "ObjectOps.hpp"
#include "objectstore/cta.pb.h"
#include "RetrieveToFileRequest.hpp"

namespace cta { namespace objectstore {
  
class Backend;
class Agent;
class GenericObject;

class Tape: public ObjectOps<serializers::Tape> {
public:
  Tape(const std::string & address, Backend & os);
  Tape(GenericObject & go);
  void initialize(const std::string & vid);
  void garbageCollect();
  bool isEmpty();
  CTA_GENERATE_EXCEPTION_CLASS(NotEmpty);
  void removeIfEmpty();
  std::string dump();
  
  // Retrieval jobs management ==================================================
  void addJob(const RetrieveToFileRequest::JobDump & job,
    const std::string & retrieveToFileAddress, uint64_t size);
  
  // -- Stored data counting ---------------------------------------------------
  uint64_t getStoredData();
  std::string getVid();
  void setStoredData(uint64_t bytes);
  void addStoredData(uint64_t bytes);
};

}}