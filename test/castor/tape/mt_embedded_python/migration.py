##########################################################################
#                                                                        #
#                           CASTOR POLICY SERVICE                        #
#                           Migration Policy File                        #
#                                                                        #
##########################################################################

# Modules
import sys
import os

def steveMigrationPolicy ():
        print "Entered steveMigrationPolicy"
        return 1;

def defaultMigrationPolicy (tapePool,castorfilename,copynb,fileId,fileSize,fileMode,uid,gid,aTime,mTime,cTime,fileClass):
        return 1;

def smallFiles(tapePool, fileSize,smallfilesTP):
        if ((fileSize <= 30000000) and (tapePool == smallfilesTP)):
            return 1
        elif ((fileSize > 30000000) and (tapePool != smallfilesTP)):
            return 1
        else:
            return 0

def migrationWithMC (tapePool,fileClass,fileSize,copynb,validcases,validcasesWithMC,smallfilesTP,rest):
        if  (tapePool, fileClass) in validcases:
             if  (smallfilesTP, fileClass) in validcases:
                 return smallFiles(tapePool,fileSize,smallfilesTP)
             else:
                 return 1
        if  (tapePool,fileClass,copynb) in validcasesWithMC:
             return 1
        validall = validcases + validcasesWithMC
        if  tapePool in rest:
                for validtuple in validall:
                        if fileClass == validtuple[1]:
                                return 0
                else:
                        return 1
        return 0

def atlasT0MigrPolicy(tapePool, castorfilename,copynb,fileId,fileSize,fileMode,uid,gid,aTime,mTime,cTime,fileClass):
        validtapepoolfileclass = [ ("atlas_recycle1w",2006), ("atlas_raw_08",2001), ("atlas_prod_08",2002), ("atlas_prod_08",2003), ("atlas_prod_08",2004)]
        validtapepoolfileclasscopynb = []
        smallfilestapepool = None
        resttapepool = [ "atlas_m4_07" ]
        myreturncode = migrationWithMC(tapePool,fileClass,fileSize,copynb,validtapepoolfileclass,validtapepoolfileclasscopynb,smallfilestapepool,resttapepool)
        f = open('/tmp/atlasT0MigrPolicy','a')
        f.writelines(str(tapePool)+' '+str(castorfilename)+' '+str(copynb)+' '+str(fileId)+' '+str(fileSize)+' '+str(fileMode)+' '+str(uid)+' '+str(gid)+' '+str(aTime)+' '+str(mTime)+' '+str(cTime)+' '+str(fileClass)+' '+str(myreturncode)+'\n')
	return myreturncode

def atlsprodMigrPolicy(tapePool, castorfilename,copynb,fileId,fileSize,fileMode,uid,gid,aTime,mTime,cTime,fileClass):
        validtapepoolfileclass = [ ("atlas_new1",139), ("atlas_new2",139), ("atlas_small_07",139), ("atlas_new1",33), ("atlas_new2",33), ("atlas_small_07",33), ("atlas_new1",59), ("atlas_new2",59), ("atlas_small_07",59), ("atlas_stream_07",145)]
        validtapepoolfileclasscopynb = [ ("atlas_new1",34,2), ("atlas_new2",34,1), ("atlas_new1",115,2), ("atlas_new2",115,1) ]
        smallfilestapepool = "atlas_small_07"
        resttapepool = [ "user_new" ]
        myreturncode = migrationWithMC(tapePool,fileClass,fileSize,copynb,validtapepoolfileclass,validtapepoolfileclasscopynb,smallfilestapepool,resttapepool)
        f = open('/tmp/atlsprodMigrPolicy','a')
        f.writelines(str(tapePool)+' '+str(castorfilename)+' '+str(copynb)+' '+str(fileId)+' '+str(fileSize)+' '+str(fileMode)+' '+str(uid)+' '+str(gid)+' '+str(aTime)+' '+str(mTime)+' '+str(cTime)+' '+str(fileClass)+' '+str(myreturncode)+'\n')
	return myreturncode

#print atlasT0MigrPolicy("atlas_t0T_new","castorfilename","copynb","fileId",1000,"fileMode","uid","gid","aTime","mTime","cTime",148)
#print atlasT0MigrPolicy("atlas_m4_07","castorfilename","copynb","fileId",1000,"fileMode","uid","gid","aTime","mTime","cTime",2)
#print atlasT0MigrPolicy("user_new","castorfilename","copynb","fileId",1000,"fileMode","uid","gid","aTime","mTime","cTime",2)
#print atlsprodMigrPolicy("atlas_new1","castorfilename","copynb","fileId",1000,"fileMode","uid","gid","aTime","mTime","cTime",139)
#print atlsprodMigrPolicy("atlas_new1","castorfilename","copynb","fileId",1000000000,"fileMode","uid","gid","aTime","mTime","cTime",139)
#print atlsprodMigrPolicy("atlas_new2","castorfilename","copynb","fileId",1000,"fileMode","uid","gid","aTime","mTime","cTime",139)
#print atlsprodMigrPolicy("atlas_new2","castorfilename","copynb","fileId",1000000000,"fileMode","uid","gid","aTime","mTime","cTime",139)
#print atlsprodMigrPolicy("atlas_small_07","castorfilename","copynb","fileId",1000,"fileMode","uid","gid","aTime","mTime","cTime",139)
#print atlsprodMigrPolicy("user_new","castorfilename","copynb","fileId",1000,"fileMode","uid","gid","aTime","mTime","cTime",2)
# End-of-File

