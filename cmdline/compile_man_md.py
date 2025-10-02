#!/usr/bin/python3

# SPDX-FileCopyrightText: 2020 CERN
#
# SPDX-License-Identifier: GPL-3.0-or-later

#
# @description  Take a file "filename.1cta.md.in" as input and create "filename.1cta.md" from
#               embedded comments in source code included with the "@include" directive

import sys
import os
import re
from datetime import datetime

# Parse source code, extracting all lines between comment delimiters /**md and */
def include_md(filename):
    with open(filename, 'r') as f:
        output = False
        for line in f:
            if re.match(r'^ *\*\/$', line):
                if output:
                    print()
                output = False
            if output:
                print(line, end='')
            if re.match(r'^ *\/\*\*md$', line):
                output = True

# Parse command line arguments
if len(sys.argv) < 2 or len(sys.argv) > 3:
    raise Exception(f"Usage: {sys.argv[0]} filename.md.in [outfile.md]")

infile = sys.argv[1]
outfile = sys.argv[2] if len(sys.argv) == 3 else re.sub(r'\.md\.in$', '.md', infile)

if not os.path.isfile(infile) or not os.access(infile, os.R_OK):
    raise Exception(f"Cannot read {infile}")
if infile == outfile:
    raise Exception(f"Input file {infile} is the same file as output file {outfile}")


# Read the input file, processing @include_md directives by including content from other files as needed
with open(infile, 'r') as fin, open(outfile, 'w') as fout:
    for line in fin:
        if re.match(r'^@include_md', line):
            filename = line.split()[1]
            with open(filename, 'r') as f:
                output = False
                for subline in f:
                    if re.match(r'^ *\*\/$', subline):
                        if output:
                            fout.write('\n')
                        output = False
                    if output:
                        fout.write(subline)
                    if re.match(r'^ *\/\*\*md$', subline):
                        output = True
        elif re.match(r'^date:$', line):
            fout.write("date: " + datetime.now().strftime('%Y-%m-%d') + "\n")
        else:
            fout.write(line)
