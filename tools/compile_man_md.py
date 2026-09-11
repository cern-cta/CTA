#!/usr/bin/python3
#
# SPDX-FileCopyrightText: 2020 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
#
# @description  Take a file "filename.1cta.md.in" as input and create "filename.1cta.md" from
#               embedded comments in source code included with the "@include" directive

import os
import re
import sys
from datetime import datetime, timezone


# Parse source code, extracting all lines between comment delimiters /**md and */
def include_md(filename: str) -> None:
    with open(filename) as f:
        output = False
        for line in f:
            if re.match(r"^ *\*\/$", line):
                if output:
                    print()
                output = False
            if output:
                print(line, end="")
            if re.match(r"^ *\/\*\*md$", line):
                output = True


# Parse command line arguments
if len(sys.argv) < 2 or len(sys.argv) > 3:
    raise ValueError(f"Usage: {sys.argv[0]} filename.md.in [outfile.md]")

infile = sys.argv[1]
outfile = sys.argv[2] if len(sys.argv) == 3 else re.sub(r"\.md\.in$", ".md", infile)

if not os.path.isfile(infile) or not os.access(infile, os.R_OK):
    raise OSError(f"Cannot read {infile}")
if infile == outfile:
    raise ValueError(f"Input file {infile} is the same file as output file {outfile}")


# Read the input file, processing @include_md directives by including content from other files as needed
with open(infile) as fin, open(outfile, "w") as fout:
    for line in fin:
        if re.match(r"^@include_md", line):
            filename = line.split()[1]
            with open(filename) as f:
                output = False
                for subline in f:
                    if re.match(r"^ *\*\/$", subline):
                        if output:
                            fout.write("\n")
                        output = False
                    if output:
                        fout.write(subline)
                    if re.match(r"^ *\/\*\*md$", subline):
                        output = True
        elif re.match(r"^date:$", line):
            source_date_epoch = os.environ.get("SOURCE_DATE_EPOCH")
            if source_date_epoch:
                build_date = datetime.fromtimestamp(int(source_date_epoch), timezone.utc).strftime("%Y-%m-%d")
                fout.write(f"date: {build_date}\n")
            else:
                fout.write(line)
        else:
            fout.write(line)
