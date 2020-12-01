# Big_Data_YACS
Team ID : BD_1439_1452_1456_1883

Team Members :
- Darshan D
- Karan Kumar G
- Manu M Bhat
- Mayur Peshve

Steps to run :

Specify the scheduling algorithm as LL/RR/RANDOM in place of "Scheduling_algorithm" <br />
python3 master.py config.json scheduling_algorithm <br />

Run the workers <br />
python3 worker.py 4000 1 <br />
python3 worker.py 4001 2 <br />
python3 worker.py 4002 3 <br />

Run the analysis file <br />
This generates the analysis.csv holding information to plot <br />
python3 analysis.py master_logfile.log ../analysis/analysis.csv <br />

To plot the bar graphs for Part 2 Result 1 <br />
python3 analysis.py master_logfile.log ../analysis/analysis.csv 1 <br />

To plot the heatmaps for Part 2 Result 2 <br />
python3 analysis.py master_logfile.log ../analysis/analysis.csv 2 <br />
