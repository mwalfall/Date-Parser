# Date Parser


This repository contains Python code that parsers a text file for dates presented in different formats. The object is to read the dates and transform them to mm/dd/yyyy format.  Examples of possible formats for dates in the text file follow:

* 04/20/2009; 04/20/09; 4/20/09; 4/3/09
* Mar-20-2009; Mar 20, 2009; March 20, 2009; Mar. 20, 2009; Mar 20 2009;
* 20 Mar 2009; 20 March 2009; 20 Mar. 2009; 20 March, 2009
* Mar 20th, 2009; Mar 21st, 2009; Mar 22nd, 2009
* Feb 2009; Sep 2009; Oct 2010
* 6/2008; 12/2009
* 2009; 2010

Dates with month and year are transformed as follows: Feb 2009 to 02/01/2009.
Dates with only the year ae transformed as followws: 2010 to 01/01/2010.

There is a Jupyter Python notebook version and a .py version. The second version extracts the text file from an Amazon S3 bucket, transforns the dates into a field named 'formatted_dates' and then loads an AWS RDS table with the data.
 
Although the text file contains less than 600 records the code is written with the assumption that a much larger file would be processed. Therefore the file is processed in a single pass.
