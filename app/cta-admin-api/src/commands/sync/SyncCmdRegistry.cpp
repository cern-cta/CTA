/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "commands/sync/SyncCmdRegistry.hpp"

#include "commands/sync/handlers/Handlers.hpp"

#include <map>
#include <utility>

namespace cta::admin_api::commands::sync {

namespace {

using admin::AdminCmd;

/// An admin command: the <Cmd, SubCmd> pair, as in the tables of the cta-admin parser
using CmdKey = std::pair<AdminCmd::Cmd, AdminCmd::SubCmd>;

const std::map<CmdKey, SyncCmdHandler> SYNC_CMD_TABLE = {
  {{AdminCmd::CMD_ADMIN, AdminCmd::SUBCMD_ADD},               handlers::addAdmin                 },
  {{AdminCmd::CMD_ADMIN, AdminCmd::SUBCMD_CH},                handlers::changeAdmin              },
  {{AdminCmd::CMD_ADMIN, AdminCmd::SUBCMD_RM},                handlers::removeAdmin              },
  {{AdminCmd::CMD_ARCHIVEROUTE, AdminCmd::SUBCMD_ADD},        handlers::addArchiveRoute          },
  {{AdminCmd::CMD_ARCHIVEROUTE, AdminCmd::SUBCMD_CH},         handlers::changeArchiveRoute       },
  {{AdminCmd::CMD_ARCHIVEROUTE, AdminCmd::SUBCMD_RM},         handlers::removeArchiveRoute       },
  {{AdminCmd::CMD_DRIVE, AdminCmd::SUBCMD_UP},                handlers::setDriveUp               },
  {{AdminCmd::CMD_DRIVE, AdminCmd::SUBCMD_DOWN},              handlers::setDriveDown             },
  {{AdminCmd::CMD_DRIVE, AdminCmd::SUBCMD_CH},                handlers::changeDrive              },
  {{AdminCmd::CMD_DRIVE, AdminCmd::SUBCMD_RM},                handlers::removeDrive              },
  {{AdminCmd::CMD_FAILEDREQUEST, AdminCmd::SUBCMD_RM},        handlers::removeFailedRequest      },
  {{AdminCmd::CMD_GROUPMOUNTRULE, AdminCmd::SUBCMD_ADD},      handlers::addGroupMountRule        },
  {{AdminCmd::CMD_GROUPMOUNTRULE, AdminCmd::SUBCMD_CH},       handlers::changeGroupMountRule     },
  {{AdminCmd::CMD_GROUPMOUNTRULE, AdminCmd::SUBCMD_RM},       handlers::removeGroupMountRule     },
  {{AdminCmd::CMD_LOGICALLIBRARY, AdminCmd::SUBCMD_ADD},      handlers::addLogicalLibrary        },
  {{AdminCmd::CMD_LOGICALLIBRARY, AdminCmd::SUBCMD_CH},       handlers::changeLogicalLibrary     },
  {{AdminCmd::CMD_LOGICALLIBRARY, AdminCmd::SUBCMD_RM},       handlers::removeLogicalLibrary     },
  {{AdminCmd::CMD_MEDIATYPE, AdminCmd::SUBCMD_ADD},           handlers::addMediaType             },
  {{AdminCmd::CMD_MEDIATYPE, AdminCmd::SUBCMD_CH},            handlers::changeMediaType          },
  {{AdminCmd::CMD_MEDIATYPE, AdminCmd::SUBCMD_RM},            handlers::removeMediaType          },
  {{AdminCmd::CMD_MOUNTPOLICY, AdminCmd::SUBCMD_ADD},         handlers::addMountPolicy           },
  {{AdminCmd::CMD_MOUNTPOLICY, AdminCmd::SUBCMD_CH},          handlers::changeMountPolicy        },
  {{AdminCmd::CMD_MOUNTPOLICY, AdminCmd::SUBCMD_RM},          handlers::removeMountPolicy        },
  {{AdminCmd::CMD_REPACK, AdminCmd::SUBCMD_ADD},              handlers::addRepack                },
  {{AdminCmd::CMD_REPACK, AdminCmd::SUBCMD_RM},               handlers::removeRepack             },
  {{AdminCmd::CMD_REQUESTERMOUNTRULE, AdminCmd::SUBCMD_ADD},  handlers::addRequesterMountRule    },
  {{AdminCmd::CMD_REQUESTERMOUNTRULE, AdminCmd::SUBCMD_CH},   handlers::changeRequesterMountRule },
  {{AdminCmd::CMD_REQUESTERMOUNTRULE, AdminCmd::SUBCMD_RM},   handlers::removeRequesterMountRule },
  {{AdminCmd::CMD_ACTIVITYMOUNTRULE, AdminCmd::SUBCMD_ADD},   handlers::addActivityMountRule     },
  {{AdminCmd::CMD_ACTIVITYMOUNTRULE, AdminCmd::SUBCMD_CH},    handlers::changeActivityMountRule  },
  {{AdminCmd::CMD_ACTIVITYMOUNTRULE, AdminCmd::SUBCMD_RM},    handlers::removeActivityMountRule  },
  {{AdminCmd::CMD_STORAGECLASS, AdminCmd::SUBCMD_ADD},        handlers::addStorageClass          },
  {{AdminCmd::CMD_STORAGECLASS, AdminCmd::SUBCMD_CH},         handlers::changeStorageClass       },
  {{AdminCmd::CMD_STORAGECLASS, AdminCmd::SUBCMD_RM},         handlers::removeStorageClass       },
  {{AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_ADD},                handlers::addTape                  },
  {{AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_CH},                 handlers::changeTape               },
  {{AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_RM},                 handlers::removeTape               },
  {{AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_RECLAIM},            handlers::reclaimTape              },
  {{AdminCmd::CMD_TAPEFILE, AdminCmd::SUBCMD_CH},             handlers::changeTapeFile           },
  {{AdminCmd::CMD_TAPEFILE, AdminCmd::SUBCMD_RM},             handlers::removeTapeFile           },
  {{AdminCmd::CMD_TAPEPOOL, AdminCmd::SUBCMD_ADD},            handlers::addTapePool              },
  {{AdminCmd::CMD_TAPEPOOL, AdminCmd::SUBCMD_CH},             handlers::changeTapePool           },
  {{AdminCmd::CMD_TAPEPOOL, AdminCmd::SUBCMD_RM},             handlers::removeTapePool           },
  {{AdminCmd::CMD_DISKSYSTEM, AdminCmd::SUBCMD_ADD},          handlers::addDiskSystem            },
  {{AdminCmd::CMD_DISKSYSTEM, AdminCmd::SUBCMD_RM},           handlers::removeDiskSystem         },
  {{AdminCmd::CMD_DISKSYSTEM, AdminCmd::SUBCMD_CH},           handlers::changeDiskSystem         },
  {{AdminCmd::CMD_DISKINSTANCE, AdminCmd::SUBCMD_ADD},        handlers::addDiskInstance          },
  {{AdminCmd::CMD_DISKINSTANCE, AdminCmd::SUBCMD_RM},         handlers::removeDiskInstance       },
  {{AdminCmd::CMD_DISKINSTANCE, AdminCmd::SUBCMD_CH},         handlers::changeDiskInstance       },
  {{AdminCmd::CMD_DISKINSTANCESPACE, AdminCmd::SUBCMD_ADD},   handlers::addDiskInstanceSpace     },
  {{AdminCmd::CMD_DISKINSTANCESPACE, AdminCmd::SUBCMD_RM},    handlers::removeDiskInstanceSpace  },
  {{AdminCmd::CMD_DISKINSTANCESPACE, AdminCmd::SUBCMD_CH},    handlers::changeDiskInstanceSpace  },
  {{AdminCmd::CMD_VIRTUALORGANIZATION, AdminCmd::SUBCMD_ADD}, handlers::addVirtualOrganization   },
  {{AdminCmd::CMD_VIRTUALORGANIZATION, AdminCmd::SUBCMD_CH},  handlers::changeVirtualOrganization},
  {{AdminCmd::CMD_VIRTUALORGANIZATION, AdminCmd::SUBCMD_RM},  handlers::removeVirtualOrganization},
  {{AdminCmd::CMD_PHYSICALLIBRARY, AdminCmd::SUBCMD_ADD},     handlers::addPhysicalLibrary       },
  {{AdminCmd::CMD_PHYSICALLIBRARY, AdminCmd::SUBCMD_CH},      handlers::changePhysicalLibrary    },
  {{AdminCmd::CMD_PHYSICALLIBRARY, AdminCmd::SUBCMD_RM},      handlers::removePhysicalLibrary    },
  {{AdminCmd::CMD_RECYCLETAPEFILE, AdminCmd::SUBCMD_RESTORE}, handlers::restoreRecycleTapeFile   },
  {{AdminCmd::CMD_ARCHIVEFILE, AdminCmd::SUBCMD_CH},          handlers::changeArchiveFile        },
};
}  // namespace

SyncCmdHandler findHandler(const admin::AdminCmd& adminCmd) {
  const auto it = SYNC_CMD_TABLE.find(CmdKey(adminCmd.cmd(), adminCmd.subcmd()));
  return it == SYNC_CMD_TABLE.end() ? nullptr : it->second;
}

}  // namespace cta::admin_api::commands::sync
