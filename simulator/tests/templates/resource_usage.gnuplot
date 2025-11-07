set terminal png size 800,600
set output '{{OUTPUT_PATH}}'
set title 'Resource Usage Over Time'
set xlabel 'Time (seconds)'
set ylabel 'Connections'
set grid
set key outside right top

# Stacked area chart - plot total first, then in_use on top
set style fill solid 0.5
plot '{{DATA_PATH}}' using 1:($2+$3) with filledcurves x1 title 'Available' linecolor rgb '#3498db', \
     '' using 1:2 with filledcurves x1 title 'In Use' linecolor rgb '#e74c3c'
