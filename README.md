# Efficiently fuzzy match strings with machine learning in PySpark

To run the example, you'll need <a href="https://virtualenv.pypa.io/en/latest/">virtualenv</a> installed

The code is implemented as a unit test that reads in 2 lists of 10 names each as a dataframe, runs the pipeline and prints out the resulting dataframe. It can be extended as needed.

Clone the repository

```
git clone https://github.com/changamire/fuzzy-string-match-pyspark.git
```

Run the following command to setup the virtual environment and run the test

```
./run.sh setup
```

After the setup has been run once, the test can subsequently be run without the <b>setup</b> flag.