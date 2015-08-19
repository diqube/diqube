#diqube query execution#

This document gives an overview of how queries are executed in diqube.

##General process overview##

1. A random diqube cluster node receives the request to execute a diql query. This node is known as **query master** in the scope of the execution of that query. Each query might have a different *query master*.
2. The query master parses the query into so-called *request* objects.
3. Then the query master optimizes and validates this object.
4. After that, the query master starts planning how to execute the query
  * The result this step is an *ExecutablePlan* (**master plan**) which is meant to be executed on the query master. 
  * An *ExecutablePlan* is made up of various *ExecutablePlanStep*s.
  * The result ExecutablePlan from above always contains a step which will send another ExecutablePlan (so-called **remote plan**) to other nodes.
5. The query master starts executing its ExecutablePlan and therefore starts distributing the remote plan to all cluster nodes in the diqube cluster which contain a part of the data of the table being queried. These cluster nodes are called *query remotes* (or just *remotes*, *remote cluster nodes*) etc.
6. All query remotes start executing the remote plan and as soon as they have results they start answering to the query master.
7. The query master in turn receives the results from the query remotes and merges them.
8. The query master sends the result of the query to the user who requested the execution of the query.

diqube tries to not only execute the query and provide the final result after the query has been executed fully, but it also tries to provide **intermediary results** to the user before the final one is available. Therefore the query remotes do not wait until they have fully processed the query, but send updates on the data as soon as they have some available. The query master in turn does the same thing - it tries to send out intermediary results as soon as some are available.

##How an ExecutablePlan works##

As stated before, an *ExecutablePlan* is made up of multiple *ExecutablePlanStep*s. Each step is executed in its own thread, each step waiting for new input data to become available. This input data is provided by previous steps: The data flow between the steps is wired when planning the execution. There are various types of data that can flow from a step to another, one example being active RowIds. Each row in a diqube table has a unique RowId, which is used to build e.g. result rows out of the columnar results of a query, as users typically expect to get a row-wise result provided. Types of steps that are available:

* *RowId source* steps: These e.g. "execute" the WHERE clause of a query and inspect the values of a specific column to match something. THey output RowIds which in the end match the whole WHERE clause.
  * `RowIdEqualsStep`
  * `RowIdInequalStep`
  * `RowIdAndStep`
  * `RowIdOrStep`
  * `RowIdNotStep`
  * `RowIdSinkStep`
* `OrderStep`
  * Orders a set of RowIds based on the values of specific columns (typically an `ORDER BY` clause) 
* `ProjectStep`
  * Projects values of a column, i.e. applies a specific function to every value and creates a new (temporary) column out of it.
  * Example: `add(columnA, 1)`
* *Grouping and aggregation* steps
  * `GroupStep`
  * `GroupIntermediaryAggregationStep`
  * `GroupFinalAggregationStep`
* *Resolving* steps:
  * `ResolveColumnDictIdsStep`
  * `ResolveValuesStep`
* *Repeated columns* steps: These are steps that work on all values of repeated columns.
  * `RepeatedProjectStep` projects the values of a repeated column into a new repeated column. Example: `add(colA[*].b, 1)`. Note that the same projection functions can be used here as can be used with the `ProjectStep`.
  * `ColumnAggregationStep` aggregates the values of a repeated column into a normal column. Example: `avg(colA[*].b)`. Note that nearly the same aggregation functions can be used here as can be used with the normal `GROUP BY` aggregation: Aggregation functions without a column-parameter (e.g. `count()`) cannot be used to aggregate a repeated column.
* *Maintanance* steps:
  * `BuildColumnFromValuesStep`
  * `ExecuteRemotePlanOnShardsStep`
  * `FilterRequestedColumnsAndActiveRowIdsStep`
  * `GroupIdAdjustingStep`
  
Some of these steps need a *column* as input, which means that other steps need to produce temporary columns. An example is when ordering on a projected column. Imagine a table that has two columns `columnA` and `columnB`, but a query being executed on it contains the following order by clause: `ORDER BY add(columnA, columnB) desc`. This is being executed by first creating a temporary column which contains the values of the sum of columnA and columnB and then the order step uses that column to resolve values of the active rowIds and uses those values to order.

The temporary columns are held in an **ExecutionEnvironment**. On the *query remotes* that ExecutionEnvironment is based on actual data of the table (which is available in a local `TableShard`), but on the *query master*, it is not. This is the case, because the query master does not need to contain any part of the data of the queried table itself - it is just used to distribute the execution to other nodes and collect and merge the results of those. On the query master, though, there are typically *VersionedExecutionEnvironment*s in place to faciliate better simultaneous execution (and therefore intermediary results can be provided to the user faster). 

##Detailed overview of execution on query master##

The following list gives an impression of what step is executed "after" another, but keep in mind that all steps are being executed simultaneously and start working as soon as more input data is available. 

1. `ExecuteRemotePlanOnShardsStep`
  * Triggers execution of remote plans on cluster nodes and collects the results
  * output `ColumnValueConsumer`
    * If grouped: to `GroupIdAdjustingStep`
    * Always:`FilterRequestedColumnsAndActiveRowIdsStep` (for values that do not need adjusted by query master but were already provided by query remotes) and all `BuildColumnFromValuesStep`s
  * output `GroupIntermediaryAggregationConsumer`
    * If grouped: to `GroupIdAdjustingStep`
  * output `RowIdConsumer`
    * If not grouped but ordered: `OrderStep`
    * If not grouped, not ordered: `FilterRequestedColumnsAndActiveRowIdsStep` and all `ResolveColumnDictIdsStep`
2. `GroupIdAdjustingStep`
  * Adjusts "groupIDs" (which are basically rowIds) from various query remotes and merges them
  * Only used when it is a grouped query
  * output `GroupIntermediaryAggregationConsumer`
    * Always: `GroupFinalAggregationStep`
  * output `RowIdConsumer`
    * If ordered: `OrderStep`
    * If not ordered: `FilterRequestedColumnsAndActiveRowIdsStep` and all `ResolveColumnDictIdsStep`
3. `BuildColumnFromValuesStep`
  * Builds a column from single column values received from query remotes
  * Only available if needed
  * output `ColumnVersionBuiltConsumer`
    * Always: to interested `ProjectStep`s.
    * Perhaps: `OrderStep` if the column is ordered by.
4. `GroupFinalAggregationStep`
  * Builds final group values from the intermediary results received from the query remotes
  * Only in place if grouped.
  * output `ColumnVersionBuiltConsumer`
    * Perhaps: a `ProjectStep` (if the group result is projected)
    * Perhaps: `OrderStep` (if the group result is ordered by)
    * If grouped value is resolved directly: `ResolveColumnDictIdsStep`
  * output `GroupFinalAggregationConsumer`
    * Read by no step directly, but by the executor, using these values to create the final result (see #41).
5. `ProjectStep`
  * Projects the values of a column to another one.
  * Only executed if a column which is aggregated is projected afterwards again. Might need the results of other (=non-aggregated) columns, though, e.g. `add(avg(columnA), 1)`.
  * output `ColumnVersionBuiltConsumer`
    * Perhaps: another `ProjectStep` (if projected multiple times)
    * Perhaps: `OrderStep` (if ordered on the projected column)
    * If projected column is resolved directly: `ResolveColumnDictIdsStep`
6. `OrderStep`
  * Orders the active rowIds according to values of columns.
  * Only in place if there's an ORDER BY.
  * output `RowIdConsumer`
    * Always `FilterRequestedColumnsAndActiveRowIdsStep` and all `ResolveColumnDictIdsStep` 
  * output `OrderedRowIdConsumer`
    * Read by no step directly, but by the executor, using these values to create the final result
7. `ResolveColumnDictIdsStep`
  * Resolves "column dict Ids" from columns on active row IDs
  * output `ColumnDictIdConsumer`
    * Always: `ResolveValuesStep`
8. `ResolveValuesStep`
  * Transforms the column dict Ids to actual result values.
  * output `ColumnValueConsumer`
9. `FilterRequestedColumnsAndActiveRowIdsStep`
  * Filters data that is in the pipeline, but is not needed.
  * output `ColumnValueConsumer`
    * Read by no step directly, but by the executor, using these values to create the final result

##Detailed overview of execution on query remotes##

TODO