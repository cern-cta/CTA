/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <string>
#include "GrpcUtils.hpp"


namespace eos::client {

void checkPrefix(std::string &prefix)
{
  if(prefix.empty()) {
    prefix = '/';
  } else {
    if(prefix.at(0) != '/') prefix = '/' + prefix;
    if(prefix.at(prefix.length()-1) != '/') prefix += '/';
  }
}


Dirname manglePathname(const std::string &remove_prefix, const std::string &add_prefix, const std::string &pathname, const std::string &filename)
{
  Dirname dir;

  // Set the pathname
  size_t clip = (pathname.rfind(remove_prefix, 0) == std::string::npos) ? 0 : remove_prefix.length();
  if(pathname.length() > clip && pathname.at(clip) == '/') ++clip;
  dir.pathname = add_prefix + pathname.substr(clip);

  // Set the filename
  if(filename.empty()) {
    clip = dir.pathname.find_last_of('/');
    dir.basename = dir.pathname.substr(clip+1);
  } else {
    if(!dir.pathname.empty() && dir.pathname.at(dir.pathname.length()-1) != '/') dir.pathname += '/';
    dir.pathname += filename;
    dir.basename = filename;
  }

  return dir;
}

} // namespace eos::client
