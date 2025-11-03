set terminal png size 800,600
set output '{{OUTPUT_PATH}}'
set title 'Processing Latency Distribution'
set xlabel 'Latency (ms)'
set ylabel 'Frequency'
set style fill solid 0.5
set grid ytics
# Automatically bin the data
set style data histogram
set style histogram clustered gap 1
set boxwidth 0.9 relative
# Use bins for histogram
binwidth = 5
bin(x,width) = width*floor(x/width) + width/2.0
plot '{{DATA_PATH}}' using (bin($1,binwidth)):(1.0) smooth freq with boxes title 'Latency'
