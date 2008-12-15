
# Script to create a Tar file with all projsct's files
# copy it to LXPLUS as a backup
# and move the tar file to a ./Backup directory


FILE=format_15_12_08_1700.tgz

tar -cvzf $FILE *.*pp Makefile setup.bash run.bash 

scp $FILE nbessone@lxplus.cern.ch:backup

mv *.tgz ./Backup

# cd /usr/local/src/CASTOR2/castor/tape/format/
