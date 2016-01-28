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

namespace cta {
// Forward declarations for opaque references.
namespace common {
namespace admin {
  class AdminHost;
  class AdminUser;
} // cta::common::admin
namespace archiveNS {
  class ArchiveFile;
  class ArchiveDirIterator;
  class ArchiveFileStatus;
} // cta::common::archiveNS 
namespace archiveRoute {
  class ArchiveRoute;
} // cta::common::archiveRoute
} // cta::common
class ArchiveToFileRequest;
class ArchiveToTapeCopyRequest;
class LogicalLibrary;
class NameServer;
class RemoteNS;
class RemotePathAndStatus;
class RetrieveRequestDump;
class RetrieveToFileRequest;
class SchedulerDatabase;
class SecurityIdentity;
class StorageClass;
class Tape;
class TapeMount;
class TapeSession;
class TapePool;
class UserIdentity;
} /// cta