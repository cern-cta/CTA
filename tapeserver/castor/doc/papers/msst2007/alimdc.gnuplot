set key top left
set yrange [0:*]
set xdata time
set timefmt "%d/%m/%Y_%H:%M:%S"
set nomxtics
set xtics "30/09/2006_00:00:00", 2628000, "01/01/2007_00:00:00" font "Times,16"
set format x "%h %Y"
set ylabel "Throughput (MB/s)" font "Times,22"
set ytics font "Times,16"
set term X11
set output
set size .7,.5
plot 'alimdcSmooth.raw' using 1:2 title 'Read I/O' with lines
replot 'alimdcSmooth.raw' using 1:3 title 'Write I/O' with lines

# eps output
set term postscript eps color
set output 'alimdc.eps'
replot

# back to screen output
set term X11
set output
