# Date Parser


The following code is an example of a data cleansing process that using regex to extract differently formatted dates from records in a text file. It locates the dates in the string, formats them and stores them in the 'formatted_dates' field. This is done in a single pass over the dataset. Examples of date formats that might occur anywhere in a string follow:
* 04/20/2009; 04/20/09; 4/20/09; 4/3/09
* Mar-20-2009; Mar 20, 2009; March 20, 2009; Mar. 20, 2009; Mar 20 2009;
* 20 Mar 2009; 20 March 2009; 20 Mar. 2009; 20 March, 2009
* Mar 20th, 2009; Mar 21st, 2009; Mar 22nd, 2009
* Feb 2009; Sep 2009; Oct 2010
* 6/2008; 12/2009
* 2009; 2010