# Automated Reporting

This repository contains the python notebook required for
running an automated report of our HHS and quality data.

The automated reporting occurs on a weekly basis and outputs an
html file (provided in the repository). 

There are some visualizations (primarily the choropleths)
that render poorly onto PDFs resulting in some minor formatting 
issues.

To produce this automated report run the following in the command
line:

```
papermill weekly-report.ipynb 2022-10-21-report.ipynb -p collection_week 2022-10-21 
jupyter nbconvert --no-input --to html 2022-10-21-report.ipynb
```

Note that the installation of papermill is required. 
And in our example command line we are creating a report for 2022-10-21. 
However, users can simply replace this date with any other appropriate date
in the dataset to create an automated report.
