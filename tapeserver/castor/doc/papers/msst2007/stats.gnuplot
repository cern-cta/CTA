set key top left
set yrange [0:*]
set xdata time
set timefmt "%d/%m/%Y"
set xrange ["01/10/2001":"11/07/2007"]
set nomxtics
set xtics "01/01/2002", 31536000, "11/07/2007" font "Times,16"
set format x "%h %Y"
set ylabel "Storage capacity (PB)" font "Times,22"
set y2tic font "Times,16"
set ytics nomirror font "Times,16"
set term X11
set output
set y2label "Number of files (millions)" font "Times,22"
set size .7,.5
plot 'STATS.LISTING.TOTAL.prod' using 1:($3/1024/1024/1024/1024/1024) axes x1y1 title 'Total size of data on tape' with lines
replot 'STATS.LISTING.TOTAL.prod' using 1:($4/1000000) axes x1y2 title 'Total number of Files' with lines

# eps output
set term postscript eps color
set output 'stats.eps'
replot

# back to screen output
set term X11
set output
