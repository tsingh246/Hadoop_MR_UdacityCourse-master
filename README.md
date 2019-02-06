# Hadoop_MR_UdacityCourse
Udacity CourseIntro to Hadoop and MapReduce

We have two part problem for practice project.

Part 1:

Sales data : purchases.txt

**************
Sample input
**************
2012-01-01	09:00	San Jose	Men's Clothing	214.05	Amex
2012-01-01	09:00	Fort Worth	Women's Clothing	153.57	Visa

Task to be implemented:

1. Instead of breaking the sales down by store, instead give us a sales breakdown by product category across all of our stores.
2. Find the monetary value for the highest individual sale for each separate store.
3. Find the total sales value across all the stores, and the total number of sales. Assume there is only one reducer.

Results for above tasks

1. Sales for,
	Toys: 57463477.11
	Consumer electronics: 57452374.13
	
2. Highest monetory value for below stores,
	Reno: 499.99
	Toledo: 499.98
	Chandler: 499.98
	
3. Number of sales and total value of sales,
	Number of sales: 4138476
	Total value: 1034457953.26
	
Part 2:

Web logs:access_log

*********************
Sample data
*******************

10.223.157.186 - - [15/Jul/2009:14:58:59 -0700] "GET / HTTP/1.1" 403 202
10.223.157.186 - - [15/Jul/2009:14:58:59 -0700] "GET /favicon.ico HTTP/1.1" 404 209
10.223.157.186 - - [15/Jul/2009:15:50:35 -0700] "GET / HTTP/1.1" 200 9157
10.223.157.186 - - [15/Jul/2009:15:50:35 -0700] "GET /assets/js/lowpro.js HTTP/1.1" 200 10469
10.223.157.186 - - [15/Jul/2009:15:50:35 -0700] "GET /assets/css/reset.css HTTP/1.1" 200 1014

Task to be implemented:
1. Hits to the page /assets/js/the-associates.js
	Count: 2456
2. Hits made by IP 10.99.99.186
3. Most Popular filname:
	Path: /assets/css/combined.css
	
	Number of occurrences: 117352
	




