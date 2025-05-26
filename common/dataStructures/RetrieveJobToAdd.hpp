/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2025 CERN
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

#include "common/dataStructures/MountPolicy.hpp"

#include <optional>
#include <string>

namespace cta::common::dataStructures {
  struct RetrieveJobToAdd {
    RetrieveJobToAdd() = default;

    RetrieveJobToAdd(uint32_t cNb, 
		     uint64_t fS,
		     std::string rra,
		     uint64_t filesize,
		     MountPolicy p,
		     time_t st,
		     std::optional<std::string> a,
		     std::optional<std::string> dsn) :
	 copyNb(cNb), fSeq(fS), retrieveRequestAddress(rra),
	 fileSize(filesize), policy(p), startTime(st), activity(a), diskSystemName(dsn) {}
    	  
    bool operator==(const RetrieveJobToAdd &rhs) const;
    bool operator!=(const RetrieveJobToAdd &rhs) const;
     
    uint32_t copyNb;
    uint64_t fSeq;
    std::string retrieveRequestAddress;
    uint64_t fileSize;
    MountPolicy policy;
    time_t startTime;
    std::optional<std::string> activity;
    std::optional<std::string> diskSystemName;
    };

std::ostream &operator<<(std::ostream &os, const RetrieveJobToAdd &obj);

}
