set terminal png size 800,600
set output '{{OUTPUT_PATH}}'
set title 'Batch Size Distribution'
set xlabel 'Batch Size'
set ylabel 'Count'
set style fill solid 0.5
set boxwidth 0.8
set grid ytics
plot '{{DATA_PATH}}' using 1:2 with boxes title 'Batches'
