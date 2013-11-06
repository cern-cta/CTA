#
#                      castor/db/cnv/CMakeLists.txt
#
# This file is part of the Castor project.
# See http://castor.web.cern.ch/castor
#
# Copyright (C) 2003  CERN
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
#
# Steven.Murray@cern.ch
#
cmake_minimum_required (VERSION 2.6)

################################################################################
# Rules to build and install libcastorcnv.so
################################################################################
set (CNV_LIB_SRC_FILES
  DbBaseAddressCnv.cpp
  DbBaseCnv.cpp
  DbChangePrivilegeCnv.cpp
  DbListPrivilegesCnv.cpp
  DbClientCnv.cpp
  DbClientIdentificationCnv.cpp
  DbDeviceGroupNameCnv.cpp
  DbDiskPoolQueryCnv.cpp
  DbFileClassCnv.cpp
  DbFiles2DeleteCnv.cpp
  DbFilesDeletedCnv.cpp
  DbNsFilesDeletedCnv.cpp
  DbFilesDeletionFailedCnv.cpp
  DbFirstByteWrittenCnv.cpp
  DbGCFileCnv.cpp
  DbGCLocalFileCnv.cpp
  DbGetUpdateDoneCnv.cpp
  DbGetUpdateFailedCnv.cpp
  DbGetUpdateStartRequestCnv.cpp
  DbMoverCloseRequestCnv.cpp
  DbPutFailedCnv.cpp
  DbPutStartRequestCnv.cpp
  DbQueryParameterCnv.cpp
  DbRequestTypeCnv.cpp
  DbSetFileGCWeightCnv.cpp
  DbStageAbortRequestCnv.cpp
  DbStageFileQueryRequestCnv.cpp
  DbStageGetRequestCnv.cpp
  DbStagePrepareToGetRequestCnv.cpp
  DbStagePrepareToPutRequestCnv.cpp
  DbStagePrepareToUpdateRequestCnv.cpp
  DbStagePutDoneRequestCnv.cpp
  DbStagePutRequestCnv.cpp
  DbStageRmRequestCnv.cpp
  DbStageUpdateRequestCnv.cpp
  DbStgFilesDeletedCnv.cpp
  DbSubRequestCnv.cpp
  DbSvcClassCnv.cpp
  DbTapeAccessSpecificationCnv.cpp
  DbTapeDriveCnv.cpp
  DbTapeDriveCompatibilityCnv.cpp
  DbTapeDriveDedicationCnv.cpp
  DbTapeRequestCnv.cpp
  DbTapeServerCnv.cpp
  DbBWUserCnv.cpp
  DbVdqmTapeCnv.cpp
  DbVersionQueryCnv.cpp
  DbNsFileIdCnv.cpp
  ../DbBaseObj.cpp
  ../DbCnvSvc.cpp
  ../DbParamsSvc.cpp
)
add_library (castorcnvs SHARED ${CNV_LIB_SRC_FILES})
CastorSetLibraryVersions (castorcnvs)
target_link_libraries (castorcnvs castorclient)
install (TARGETS castorcnvs LIBRARY DESTINATION ${CASTOR_DEST_LIB_DIR}
  NAMELINK_SKIP)
