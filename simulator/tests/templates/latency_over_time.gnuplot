set terminal png size 800,600
set output '{{OUTPUT_PATH}}'
set title 'Latency Over Time'
set xlabel 'Time (seconds)'
set ylabel 'Latency (ms)'
set grid
set key outside right top

# Plot p99, mean, and p50
plot '{{DATA_PATH}}' using 1:4 with lines linewidth 2 linecolor rgb '#e74c3c' title 'p99', \
     '' using 1:2 with lines linewidth 2 linecolor rgb '#3498db' title 'Mean', \
     '' using 1:3 with lines linewidth 2 linecolor rgb '#2ecc71' title 'p50'
