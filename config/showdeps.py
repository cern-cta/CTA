import os

# create a temporary file
tmpFile = os.tmpfile()

# fill it with constant stuff
tmpFile.write("#define YES 1\n")
tmpFile.write("#define NO 0\n\n")
tmpFile.write("#include \"site.def\"\n")
tmpFile.write("#include \"Project.tmpl\"\n\n")
tmpFile.write("#undef YES\n")
tmpFile.write("#undef NO\n\n")

# open site.def and parse it
sitedef = open("config/site.def")
entries = []
for l in sitedef.readlines():
    tokens = l.split()
    if len(tokens) > 2 and tokens[0] == "#define" and tokens[2] in ["YES", "NO"]:
        if tokens[1] not in entries:
            entries.append(tokens[1])
sitedef.close()

# put entries in the tmpFile
maxEntryLen = 0
for e in entries:
    if len(e) > maxEntryLen: maxEntryLen = len(e)
    tmpFile.write("\"" + e + "\" " + e + "\n")
tmpFile.seek(0)

# run cpp on the tmp file
cmd = "/usr/bin/cpp -I config "
(input, output) = os.popen2 (cmd, "rt")
input.write(tmpFile.read())
tmpFile.close()
input.close()

# output the result
res = ""
for l in output.readlines():
    toks = l.split()
    if len(toks) > 1 and toks[1] in ["YES", "NO"]:
        print toks[0][1 : len(toks[0]) - 1].ljust(maxEntryLen), toks[1]
output.close()

