/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <list>
#include <map>
#include <stdint.h>
#include <string>


namespace cta::common::dataStructures {

/**
 * This struct holds all the data necessary to rebuild the original disk based
 * file in case of disaster
 */
struct DiskFileInfo {

  DiskFileInfo();

  DiskFileInfo(const std::string &path, uint32_t owner_uid, uint32_t gid);

  bool operator==(const DiskFileInfo &rhs) const;

  bool operator!=(const DiskFileInfo &rhs) const;

  std::string path;
  uint32_t    owner_uid;
  uint32_t    gid;

}; // struct DiskFileInfo

std::ostream &operator<<(std::ostream &os, const DiskFileInfo &obj);

} // namespace cta::common::dataStructures
