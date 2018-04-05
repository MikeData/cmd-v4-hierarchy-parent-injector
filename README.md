# cmd-v4-hierarchy-parent-injector

Injects totals of all dimension combinations for missing hierarchical parent dims into the v4 csv.

Takes one hierarchy.csv file (the 4 columns CSVs used by customise my data hierarchy transformer). Example: https://raw.githubusercontent.com/ONSdigital/dp-hierarchy-builder/cmd-develop/cmd/hierarchy-transformer/sic07-heirarchy.csv

And one V4 load file.


# Installation

Install from this git repo with:

` pip install git+https://github.com/ONS-OpenData/cmd-v4-hierarchy-parent-injector.git`


# Usage

You'll need to specify the hierarchy.csv, the V4 file and the codelist they have in common.

```
from hierarchyParentInjector.injector import injectParents

injectParents(hierarchy.csv, V4.csv, "codelist in question")
```

If the script completes without errors a v4 file will be created with the same name as the input v4 prefixed "ParentsInjected_".


# Additional Options

The 'injectParents' function takes several keyword arguments as list below:


`time=`   

The dimension that holds the actual time value. The script will tell you if you need to change this (i.e it can't find the specified default column). The default is "time"


`geography=`

The dimension that holds the geographic codes. The script will tell you if you need to change this (i.e it can't find the specified default column). The default is "geography_codelist".


`populatePrimeNode=`

Do you want to insert a total for the very first node of the hierarchy if its the only node on its tier. Usually False as filtering to select all is arguably self defeating. The  default is "False" 

