import sys
import os

def migrationTestPolicyFunction (tapepool,castorfilename,copynb,fileId,fileSize,fileMode,uid,gid,aTime,mTime,cTime,fileClass):
        f = open('migrationTestPolicyFunctionOutput.txt','w')
        f.writelines('tapepool       = "' + str(tapepool)       + '"\n')
        f.writelines('castorfilename = "' + str(castorfilename) + '"\n')
        f.writelines('copynb         = '  + hex(copynb)         + '\n')
        f.writelines('fileId         = '  + hex(fileId)         + '\n')
        f.writelines('fileSize       = '  + hex(fileSize)       + '\n')
        f.writelines('fileMode       = '  + hex(fileMode)       + '\n')
        f.writelines('uid            = '  + hex(uid)            + '\n')
        f.writelines('gid            = '  + hex(gid)            + '\n')
        f.writelines('aTime          = '  + hex(aTime)          + '\n')
        f.writelines('mTime          = '  + hex(mTime)          + '\n')
        f.writelines('cTime          = '  + hex(cTime)          + '\n')
        f.writelines('fileClass      = '  + hex(fileClass)      + '\n')
        return 1;
