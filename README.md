####SQL on CSV

Let user run, SQL queries on their .csv files. Current target is to support all type of `SELECT` queries.

Supports
--------

* WHERE queries with all operators
* MAX, MIN, AVG, SUM, DISTINCT functions
* Multiple tables
* Multiple table joins
* Partial select using table names

Input format
------------

* Table data in .csv
* Table header info in metadata.txt (check sample metadata.txt for more info)


Thanks to Database Systems course.
(code needs a lot of refactoring :P )
