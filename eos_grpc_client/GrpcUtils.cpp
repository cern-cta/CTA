/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <string>
#include "GrpcUtils.hpp"


namespace eos {
namespace client {

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

}} // namespace eos::client
