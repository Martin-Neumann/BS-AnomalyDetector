# BS-AnomalyDetector

| Date        | Notes  |
| ------------- |:-------------:|
| 14-04-2016    | Initial upload. The code is transfered from an internal repository and will be gradeually uploaded here.|

BS-AnomalyDetector is a libary for [Apache Flink](https://flink.apache.org/) streaming. It provides the bilding blocks to create a distributed fault tolerant streaming anomaly detection pipeline in flink using Bayesian Statistical Anomaly.

## Bayesian Statistical Anomaly

1. Find a distribution the values you want to detect anomalies on follow. Currently supported distributions are Gaussian, Poisson and Exponential. 
2. Create windows from the stream and store the values defining the distribution. For example for a Poisson distribution the number of events and the sum of the values needs to be stored. 
3. Check if the data from the current window can come from the same distribution defined by windows seen in the past.

Strenght of the approach:

1. Compact model: The only data that needs to be stored is the values that define the distributions. This allows to keep a huge number of models in memory.
2. (Almost) no training time needed for the model: This approach compares current data to data seen before, besindes a startup phase where is has not seen enough previous data yet it does not need any training.
3. No Golden truth needed to setup: Many approaches requore a golden truth dataset to learn what is a normal case. Since this approach is purely build on statistics assuming the normal case is more frequrent then the abnormal case this is not nessesary.

Downsides of the approach:

1. If the data must fit the choosen distribution to give good results
2. Anomalies might be missed if they "hide" in the window aggregation. If there are extremely high values and extremely low values in the same window they might even out to a normal value.
3. The size of the window and history need to be choosen carefully and have a huge impact on the outcome.

## Use the libary

Work in progress

## About

This work is based on research done by Anders Holst: 
- Holst, Anders, et al. "Statistical anomaly detection for train fleets." AI Magazine 34.1 (2012): 33.
- Holst, Anders, and Jan Ekman. "Incremental Stream Clustering for Anomaly Detection and Classification." SCAI. 2011.
