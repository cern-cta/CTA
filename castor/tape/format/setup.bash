
#!/bin/sh

TMP_DIR=/root/nbessone/project/ReadWriteFile/castor/tape/format/required_so:/root/nbessone/project/ReadWriteFile/castor/tape/format

export PATH=${PATH}:$TMP_DIR
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}$TMP_DIR

echo "PATH: ${PATH}"
echo "LD_LIBRARY_PATH: ${LD_LIBRARY_PATH}"
