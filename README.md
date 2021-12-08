# KommatiTrading

This application is used by **KommatiPara** to join client data and financial data into one dataset
and filter on clients from particular countries.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install the necessary requirements

```bash
pip install -r requirements
```

## Usage

To run the main script:
- go to the src/ directory
- run the script via spark-submit, pass the script and 3 parameters which are as follows:

spark-submit main.py p1 p2 "p3"

p1: the full path of the first dataset \
p2: the full path of the second dataset \
p3: a list of countries to filter on separated by a comma and in open and closed quotations

Example:
```bash
spark-submit main.py C:\Work\ABN-Ambro\KommatiTrading\input_data\dataset_one.csv C:\Work\ABN-Ambro\KommatiTrading\input_data\dataset_two.csv "United Kingdom,Netherlands"
```

## Testing
To execute the unit testing module run the following command under root directory

```bash
python -m unittest tests.test_main
```