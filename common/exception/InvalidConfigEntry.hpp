/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
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

#pragma once

#include "common/exception/Exception.hpp"

#include <string>


namespace cta { namespace exception {

    /**
     * Invalid configuration entry exception.
     */
    class InvalidConfigEntry : public cta::exception::Exception {
      
    public:
      
      /**
       * Constructor
       *
       * @param catagory   The category of the configuration entry.
       * @param entryName  The name of the configuration entry.
       * @param entryValue The (invalid) value of the configuration entry.
       */
      InvalidConfigEntry(const char *const category,
        const char *const entryName, const char *const entryValue);

      /**
       * Trivial, but explicitely non-throwing destructor (required through
       * inheritence from std::exception) 
       */
      virtual ~InvalidConfigEntry() {};
      
      /**
       * Returns the category of the configuration entry.
       */
      const std::string &getEntryCategory();

      /**
       * Returns the name of the configuration entry.
       */
      const std::string &getEntryName();

      /**
       * Returns the (invalid) value of the configuration entry.
       */
      const std::string &getEntryValue();


    private:

      /**
       * The category of the configuration entry.
       */
      const std::string m_entryCategory;

      /**
       * The name of the configuration entry.
       */
      const std::string m_entryName;

      /**
       * The (invalid) value of the configuration entry.
       */
      const std::string m_entryValue;

    }; // class InvalidConfigEntry

} } // namespace cta exception

