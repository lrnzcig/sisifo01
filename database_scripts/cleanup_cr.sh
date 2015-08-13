#!/bin/bash

# to make it work in mac as in linux, use gsed instead of sed
#
# info:
# \r cr in linux
# \n cr in mac
# \r\n cr in windows
#
# old files: \r\n is real cr, \n should be removed
#

FILE_NAME=$1

# cat $FILE_NAME | od -c

tr -d '\n' < ${FILE_NAME} > ${FILE_NAME}.remove
gsed 's/\r/\r\n/g' ${FILE_NAME}.remove > ${FILE_NAME}.cr
rm ${FILE_NAME}.remove
