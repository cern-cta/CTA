#!/bin/sh
sed 's/^END;/END;\n\//' $1 | sed 's/^\(END castor[a-zA-Z]*;\)/\1\n\//' | sed 's/\(CREATE OR REPLACE TYPE .*\)$/\1\n\//' > $1plus
