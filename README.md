# Big_Data_YACS
Team ID : BD_1439_1452_1456_1883

Team Members :
- Darshan D
- Karan Kumar G
- Manu M Bhat
- Mayur Peshve

Steps to run :

Specify the scheduling algorithm as LL/RR/RANDOM in place of "Scheduling_algorithm"
python3 master.py config.json scheduling_algorithm

Run the workers
python3 worker.py 4000 1
python3 worker.py 4001 2
python3 worker.py 4002 3

Run the analysis file
This generates the analysis.csv holding information to plot
python3 analysis.py master_logfile.log ../analysis/analysis.csv

To plot the bar graphs for Part 2 Result 1
python3 analysis.py master_logfile.log ../analysis/analysis.csv 1

To plot the heatmaps for Part 2 Result 2
python3 analysis.py master_logfile.log ../analysis/analysis.csv 2
