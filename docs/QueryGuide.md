#diqube - Query Guide#

This document gives an overview over what type of queries diqube supports.

##diql##

diqube can execute queries that are written in diql - **di**qube **q**uery **l**anguage.

diql supports `select` statements which are similar to those of SQL. These statements can *resolve*, *project*, *aggregate*, *filter* and *order* data. As each row in diqube can contain a complex object which also may contain repeated data, both *projections* and *aggregations* can be executed on those repeated columns, too.

##Examples##

Simple selection:
```
select columnA 
from tableA
``` 

Selection with filter:
```
select columnA 
from tableA 
where columnB > 0
```

Selecting a projection after filtering on another projection:
```
select add(columnA, columnB) 
from tableA 
where sub(columnA, columnB) < 10
```

Selecting and aggregating rows:
```
select columnA, avg(columnB) 
from tableA 
group by columnA
```

Selecting, aggregating and filtering on those aggregations:
```
select columnA, avg(columnB) 
from tableA 
group by columnA 
having avg(columnB) < 5.
```

Projecting and aggregating repeated columns:
```
select columnA, sum(avg(columnB[*])) from tableA where sum(avg(columnB[*])) = 5.
```

First aggregating a repeated column and then aggregating these values row-wise:
```
select columnA, min(avg(columnB[*])) from tableA group by columnA
```

##Row-wise aggregation vs. column-wise aggregation##

Each "row" in a diqube table can store a complex object which may contain repeated fields or "arrays". Assume the two following JSON objects are stored in an example table, each JSON object representing one *row* in diqube:

```
row1 = 
{
  "columnA" : "1",
  "columnB" : [
                0,
                5,
                10
              ],
  "columnC" : 1000
}
```

```
row2 = 
{
  "columnA" : "2",
  "columnB" : [
                100,
                150,
                200
              ],
  "columnC" : 2500
}
```

```
row3 = 
{
  "columnA" : "1",
  "columnB" : [
                50,
                75,
                100
              ],
  "columnC" : 5000
}
```

The usual SQL-like aggregation is a **row-wise aggregation**, which means that the aggregation function aggregates the values of a specific column of a set of rows. Example:

```
select columnA, avg(columnC) from ... group by columnA
```

This statement will then aggregate `row1` and `row3` because they have the same value in `columnA` and then it calculates the average of these two rows. The result table would be:

columnA | avg(columnC)
--------|-------------
1       | 2500.
2       | 2500.

The **column-wise aggregation** on the other hand aggregates the values of multiple columns (read: multiple values of the same repeated column) of a single row. This is triggered by not using the `group by` clause, but instead referencing the repeated column using the special syntax `[*]` (which reads as "all values of that repeated column"):

```
select columnA, avg(columB[*]) from ...
```

The result will be three rows:

columnA | avg(columnB)
--------|-------------
1       | 5.
2       | 150.
1       | 75.

On top of that column-wise aggregation you can execute a row-wise aggregation, of course:

```
select columnA, min(avg(columB[*])), max(avg(columB[*])) from ... group by columnA
```

columnA | min(avg(columB[*])) | max(avg(columB[*]))
--------|---------------------|--------------------
1       | 5.                  | 75.
2       | 150.                | 150.

For a column-wise aggregation you can use the same aggregation functions as for the row-wise aggregation, but only not use those aggregation function which doe not have a column-parameter (e.g. you can't use `count()` for a column-wise aggregation).

##Repeated projection##

Similar to the above differentiation of row-wise and column-wise aggregation, a projection can also be executed on all values of a repeated column, using the same syntax:

```
row1 = 
{
  "columnA" : "1",
  "columnB" : [
                0,
                5,
                10
              ]
}
```

From a logical point of view, when executing the projection function `add(columnB[*], 1)` on that object you would have the result object:  

```
row1* = 
{
  "columnA" : "1",
  "columnB" : [
                1,
                6,
                11
              ]
}
```
 You though cannot *select* a repeated column, but you first have to aggregate it to a single value. You therefore could execute the following on `row1` (not `row1*`), which includes both a repeated projection and a column-wise aggregation:
 
```
select columnA, sum(add(columB[*], 1))
```

columnA | sum(add(columB[*], 1))
--------|-----------------------
1       | 18

##Nested objects##

Of course diqube also supports selecting from nested objects:

```
row1 = 
{
  "columnA" : "1",
  "columnB" : {
                "x" : 0,
                "y" : 5,
                "z" : 10
              }
}
```

Simply use a `.` notation to select from nested objects:

```
select columnA, columnB.y from ...
```

columnA | columnB.y
--------|-----------------------
1       | 5

Of course you also can nest objects in repeated fields:

```
row1 = 
{
  "columnA" : "1",
  "columnB" : [ {
                  "x" : 0,
                  "y" : 5
                },
                {
                  "x" : -100,
                  "y" : 10
                } ]
}
```

```
select columnA, avg(columnB[*].y) from ...
```

columnA | avg(columnB[*].y)
--------|-----------------------
1       | 7.5

##Special columns##

###Repeated columns###

Repeated columns are flattened on import, which means that each entry in the input "array" gets a custom column:
```
row1 = 
{
  "columnA" : "1",
  "columnB" : [ {
                  "x" : 0,
                  "y" : 5
                },
                {
                  "x" : -100,
                  "y" : 10
                } ]
}
```
This will map internally in diqube to the following table:

columnA | columnB[0].x | columnB[0].y | columnB[1].x | columnB[1].y | columnB[length]
--------|--------------|--------------|--------------|--------------|----------------
1       | 0            | 5            | -100         | 10           | 2

You see that each entry in the array gets own columns which are indexed using a number. In addition to that there is one column added automatically which holds the length of the array for a specific row. You can access these columns in queries, too, but you should be very careful accessing the indexed columns, as each row might have a different length and the returned values might not be valid for a specific row. 

```
select columnA, columnB[length] from ...
```

columnA | columB[length]
--------|---------------
1       | 2

##Data types##

diqube internally supports 3 data types: `STRING`, `LONG`, `DOUBLE`. Each column in the table has a data type and each function (both projection/aggregation) executed on it has an input data type and an output data type. 

For example the `avg` aggregation function exists in two flavors:
  * Input: `LONG`, Ouptut: `DOUBLE`
  * Input: `DOUBLE`, Ouptut: `DOUBLE`

In the examples above you can see sometimes trailing `.` characters, which is a short form of writing `.0` - this denotes a `DOUBLE` data type.
To convert a `DOUBLE` to `LONG` you could use the `round` projection function.