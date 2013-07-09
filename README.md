OozieProject-CoordinatorJob-TimeTriggered
=========================================

This project includes components of a oozie coordinator job - scripts/code, sample data and commands;  Oozie actions covered: hdfs action, email action, java main action,  hive action;  Oozie controls covered: decision, fork-join; The workflow includes a sub-workflow that runs two hive actions concurrently.  The hive table is partitioned; Parsing uses hive-regex serde, and Java-regex.  Also, the java mapper, gets the input  directory path and includes part of it in the key.

For details on the data, scripts, commands and output/results, read my gist at:
https://gist.github.com/airawat/5954657
