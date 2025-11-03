set terminal png size 800,600
set output '{{OUTPUT_PATH}}'
set title 'Arrival Rate Over Time'
set xlabel 'Time (seconds)'
set ylabel 'Requests Per Second (RPS)'
set grid
plot '{{DATA_PATH}}' using 1:2 with lines linewidth 2 title 'RPS'
