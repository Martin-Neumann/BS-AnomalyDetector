# BS-AnomalyDetector

| Date          | Notes         |
| ------------- |:-------------:|
| 02-05-2016    | Updated interface, hideing the preprocessing step.|
| 23-04-2016    | Improved testcoverage, added Normal and Lognormal distributions.|
| 14-04-2016    | Initial upload. The code is transfered from an internal repository and will be gradeually uploaded here.|

BS-AnomalyDetector is a libary for [Apache Flink](https://flink.apache.org/) streaming. It provides the building blocks to create a distributed fault tolerant streaming anomaly detection pipeline in flink using Bayesian Statistical Anomaly.

## Bayesian Statistical Anomaly

1. Find out what distribution the value you want to detect anomalies for follows. Currently supported distributions are Normal, LogNormal, Poisson and Exponential. 
2. Create windows from the stream and store the values defining the distribution. For example for a Poisson distribution the number of events and the sum of the values needs to be stored. 
3. Check if the data from the current window can come from the same distribution defined by windows seen in the past.

Strength of the approach:

1. Compact model: The only data stored e isare the defineing values for the distributions. This allows to keep a huge number of models in memory.
2. (Almost) no training time needed for the model: This approach compares current data to data seen before, it has a startup phase where it will fill the history. Otherwise no training phase is needed.
3. No Golden truth needed: This is a statistical approach; assuming normal cases are more frequrent then abnormal cases. It does not require a Golden thruth dataset to set up.

Limitations of the approach:

1. If the data must fit the choosen distribution to give good results
2. Anomalies might be missed if they "hide" in the window aggregation. If there are extremely high values and extremely low values in the same window they might even out to a normal value.
3. The size of the window and history need to be choosen carefully and have a huge impact on the outcome.

## Use the libary


Work in progress

## About

This work is based on research done by Anders Holst: 
- Holst, Anders, et al. "Statistical anomaly detection for train fleets." AI Magazine 34.1 (2012): 33.
- Holst, Anders, and Jan Ekman. "Incremental Stream Clustering for Anomaly Detection and Classification." SCAI. 2011.
