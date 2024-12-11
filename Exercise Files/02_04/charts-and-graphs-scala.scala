// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC # **Chart and Graph Types with Scala**
// MAGIC This notebook covers the various charts and graphs that are built into Databricks.
// MAGIC
// MAGIC While Scala is used to generate the test data displayed in the visualizations in this notebook, all the information about how to configure these charts & graphs applies to all notebooks.

// COMMAND ----------

// MAGIC %md  ### A **Table View** is the most basic way to view data.
// MAGIC * Only the first 1000 rows will be displayed in the table view.

// COMMAND ----------

case class MapEntry(key: String, value: Int)
val largeSeries = for (x <- 1 to 5000) yield MapEntry("k_%04d".format(x), x)
val largeDataFrame = sc.parallelize(largeSeries).toDF()
largeDataFrame.registerTempTable("largeTable")
display(sqlContext.sql("select * from largeTable"))

// COMMAND ----------

// MAGIC %md ### Tables in Databricks Cloud are configured with **Plot Options...**.
// MAGIC * The **Keys** section is for specifying the control variable which is typically displayed as the X-Axis on many of the graph types.  Most graphs can plot about 1000 values for the keys, but again - it varies for different graphs.
// MAGIC * The **Values** section is for specifying the observed variable and is typically displayed on the Y-Axis.  This also tends to be an observed numerical value on most graph types.
// MAGIC * The **Series groupings** section is for specifying ways to break out the data - for a bar graph - each series grouping has a different color for the bars with a legend to denote that value of each series grouping.  Many of the graph types can only handle series groupings that has 10 or less unique values. 
// MAGIC
// MAGIC **Some graph types also specifying even more options - and those will be discussed as applicable.**

// COMMAND ----------

// MAGIC %md ### A **Pivot Table** is another way to view data in a table format.
// MAGIC Instead of just returning the raw results of the table - it can automatically sort, count total or give the average of the data stored in the table.
// MAGIC * Read more about Pivot Tables here: http://en.wikipedia.org/wiki/Pivot_table
// MAGIC * For a Pivot Table, key, series grouping and value fields can be specified.   
// MAGIC * The **Key** is the first column, and there will be one row per key in the Pivot Table.
// MAGIC * There will be additional column for each unique value for the **Series Grouping**.
// MAGIC * The table will contain the **Values** field in the cells.  Value must be a numerical field that can be combined using aggregation functions.
// MAGIC * Cell in the Pivot Table are calculated from multiple rows of the original table.
// MAGIC   * Select **SUM**, **AVG**, **MIN**, **MAX**, or **COUNT** as the way to combine the original rows into that cell.
// MAGIC * Pivoting is done on the server side of Databricks Cloud to calculate the cell values.

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC To create a Pivot Table, click on the Graph Icon below a result and select **Pivot**:
// MAGIC
// MAGIC ![plot-menu-pick-pivot](https://docs.databricks.com/_static/images/notebooks/plot-menu-pick-pivot.png)

// COMMAND ----------

// Click on the Plot Options Button...to see how this pivot table was configured.
// NOTE how Pivot Tables are highlighted in green to distinguish them from regular charts.
case class PivotEntry(key: String, series_grouping: String, value: Int)
val largePivotSeries = for (x <- 1 to 5000) yield PivotEntry("k_%03d".format(x % 200),"group_%01d".format(x % 3), x)
val largePivotDataFrame = sc.parallelize(largePivotSeries).toDF()
largePivotDataFrame.registerTempTable("table_to_be_pivoted")
display(sqlContext.sql("select * from table_to_be_pivoted"))

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Another way to think of a pivot table is that it does a group by on your original table by the key & series grouping, but instead of outputting (key, series_grouping, aggregation_function(value)) tuples, it outputs a table where the schema is the key and every unique value for the series grouping.
// MAGIC * See the results of group_by statement below, which contains all the data that is in the pivot table above.

// COMMAND ----------

// MAGIC %sql select key, series_grouping, sum(value) from table_to_be_pivoted group by key, series_grouping order by key, series_grouping

// COMMAND ----------

// MAGIC %md ### A **Bar Chart** is a type of visual pivot table graph and a great basic way to visualize data.
// MAGIC * **Plot Options...** was used to configure the graph below.
// MAGIC * The **Key** is *Year* and appears on the X-Axis.
// MAGIC * The **Series groupings** is *Category* and there is a different color to denote each of those.
// MAGIC * The **Values** is *salesAmount* and appears on the Y-Axis.
// MAGIC * **Sum** was selected as the aggregation method, which means sales amounts are summed for all the products in the category for that year.

// COMMAND ----------

case class SalesEntry(category: String, product: String, year: Int, salesAmount: Double)
val salesEntryDataFrame = sc.parallelize(
  SalesEntry("fruits_and_vegetables", "apples", 2012, 100.50) :: 
  SalesEntry("fruits_and_vegetables", "oranges", 2012, 100.75) :: 
  SalesEntry("fruits_and_vegetables", "apples", 2013, 200.25) :: 
  SalesEntry("fruits_and_vegetables", "oranges", 2013, 300.65) :: 
  SalesEntry("fruits_and_vegetables", "apples", 2014, 300.65) :: 
  SalesEntry("fruits_and_vegetables", "oranges", 2015, 100.35) ::
  SalesEntry("butcher_shop", "beef", 2012, 200.50) :: 
  SalesEntry("butcher_shop", "chicken", 2012, 200.75) :: 
  SalesEntry("butcher_shop", "pork", 2013, 400.25) :: 
  SalesEntry("butcher_shop", "beef", 2013, 600.65) :: 
  SalesEntry("butcher_shop", "beef", 2014, 600.65) :: 
  SalesEntry("butcher_shop", "chicken", 2015, 200.35) ::
  SalesEntry("misc", "gum", 2012, 400.50) :: 
  SalesEntry("misc", "cleaning_supplies", 2012, 400.75) :: 
  SalesEntry("misc", "greeting_cards", 2013, 800.25) :: 
  SalesEntry("misc", "kitchen_utensils", 2013, 1200.65) :: 
  SalesEntry("misc", "cleaning_supplies", 2014, 1200.65) :: 
  SalesEntry("misc", "cleaning_supplies", 2015, 400.35) ::
  Nil).toDF()
salesEntryDataFrame.registerTempTable("test_sales_table")
display(sqlContext.sql("select * from test_sales_table"))

// COMMAND ----------

// MAGIC %md **Tip:** Hover over each bar in the chart below to see the exact values plotted.

// COMMAND ----------

// MAGIC %md ### A **Line Graph** is another example of a pivot table graph that can highlight trends for your data set.
// MAGIC * **Plot Options...** was used to configure the graph below.
// MAGIC * The **Key** is *Year* and appears on the X-Axis.
// MAGIC * The **Series groupings** is *Product* and there is different color to denote each of those.
// MAGIC * The **Values** is *salesAmount* and appears on the Y-Axis.
// MAGIC * **Sum** was selected as the aggregation method, which means sales amounts are summed for all the products in the category for that year.

// COMMAND ----------

// MAGIC %sql select cast(string(year) as date) as year, category, salesAmount from test_sales_table

// COMMAND ----------

// MAGIC %md ### A **Pie Chart** is pivot table graph type that can allow you to see what percentage of the whole your values represent.
// MAGIC * **NOTE:** As opposed to the previous examples, Key & Series Groupings have been switched.
// MAGIC * **Plot Options...** was used to configure the graph below.
// MAGIC * The **Key** is *Category* and one color is used for each product.
// MAGIC * The **Series groupings** is *Year* and there is different pie chart for each year.
// MAGIC * The **Values** is *salesAmount* and is used to calculate the percentage of the pie.
// MAGIC * **Sum** is selected as the aggregation method.
// MAGIC

// COMMAND ----------

// MAGIC %sql select * from test_sales_table

// COMMAND ----------

// MAGIC %md ### A **Map Graph** is a way to visualize your data on a map.
// MAGIC * **Plot Options...** was used to configure the graph below.
// MAGIC * **Keys** should contain the field with the location.
// MAGIC * **Series groupings** is always ignored for World Map graphs.
// MAGIC * **Values** should contain exactly one field with a numerical value.
// MAGIC * Since there can multiple rows with the same location key, choose "Sum", "Avg", "Min", "Max", "COUNT" as the way to combine the values for a single key.
// MAGIC * Different values are denoted by color on the map, and ranges are always spaced evenly.
// MAGIC   * Apply a smoothing function to your graph if your values are not evenly distributed.

// COMMAND ----------

// MAGIC %md To plot just a graph of the US by state, use US state postal codes as the key.

// COMMAND ----------

case class StateEntry(state: String, value: Int)
val stateRDD = sc.parallelize(
  StateEntry("MO", 1) :: StateEntry("MO", 10) ::
  StateEntry("NH", 4) ::
  StateEntry("MA", 8) ::
  StateEntry("NY", 4) ::
  StateEntry("CA", 7) ::  Nil).toDF()
stateRDD.registerTempTable("test_state_table")
display(sqlContext.sql("Select * from test_state_table"))

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC To plot a graph of the world, use [country codes in ISO 3166-1 alpha-3 format](http://en.wikipedia.org/wiki/ISO_3166-1_alpha-3) as the key.

// COMMAND ----------

// Reminder: It's not a requirement to register this RDD as a temp table for Spark SQL - display can also be called directly on the RDD. 
case class WorldEntry(country: String, value: Int)
val worldRDD = sc.parallelize(
  WorldEntry("USA", 1000) ::
  WorldEntry("JPN", 23) ::
  WorldEntry("GBR", 23) ::
  WorldEntry("FRA", 21) ::
  WorldEntry("TUR", 3) ::
  Nil).toDF()
display(worldRDD)

// COMMAND ----------

// MAGIC %md ### A **Scatter Plot** allows you to see if there is a correlation between two variables.
// MAGIC * **Plot Options...** was selected to configure the graph below.
// MAGIC * **Keys** will be used to color the points on the graphs - with a legend on the side.
// MAGIC * **Series Grouping** is ignored.
// MAGIC * **Value** must contain at least two numberical fields.  This graph has a, b, and c as the values.
// MAGIC * The diagonal of the resulting plot is the kernal density plot of the variable.
// MAGIC * The row always has the variable in the Y-Axis, and the column has the variable in the X-Axis.

// COMMAND ----------

case class ScatterPlotEntry(key: String, a: Double, b: Double, c: Double)
val scatterPlotRDD = sc.parallelize(
  ScatterPlotEntry("k1", 0.2, 120, 1) :: ScatterPlotEntry("k1", 0.4, 140, 1) :: ScatterPlotEntry("k1", 0.6, 160, 1) :: ScatterPlotEntry("k1", 0.8, 180, 1) ::
  ScatterPlotEntry("k2", 0.2, 220, 1) :: ScatterPlotEntry("k2", 0.4, 240, 1) :: ScatterPlotEntry("k2", 0.6, 260, 1) :: ScatterPlotEntry("k2", 0.8, 280, 1) ::
  ScatterPlotEntry("k1", 1.2, 120, 1) :: ScatterPlotEntry("k1", 1.4, 140, 1) :: ScatterPlotEntry("k1", 1.6, 160, 1) :: ScatterPlotEntry("k1", 1.8, 180, 1) ::
  ScatterPlotEntry("k2", 1.2, 220, 2) :: ScatterPlotEntry("k2", 1.4, 240, 2) :: ScatterPlotEntry("k2", 1.6, 260, 2) :: ScatterPlotEntry("k2", 1.8, 280, 2) ::
  ScatterPlotEntry("k1", 2.2, 120, 1) :: ScatterPlotEntry("k1", 2.4, 140, 1) :: ScatterPlotEntry("k1", 2.6, 160, 1) :: ScatterPlotEntry("k1", 2.8, 180, 1) ::
  ScatterPlotEntry("k2", 2.2, 220, 3) :: ScatterPlotEntry("k2", 2.4, 240, 3) :: ScatterPlotEntry("k2", 2.6, 260, 3) :: ScatterPlotEntry("k2", 2.8, 280, 3) ::
  Nil).toDF()
display(scatterPlotRDD)

// COMMAND ----------

// MAGIC %md #### LOESS Fit Curves for Scatter Plots
// MAGIC
// MAGIC [LOESS](https://en.wikipedia.org/wiki/Local_regression) is a method of performing local regression on your data to produce a smooth estimation curve that describes the data trend of your scatter plot. It does this by interpolating a curve within its neighborhood of data points. The LOESS fit curve is controlled by a bandwidth parameter that specifies how many neighboring points should be used to smooth the plot. A high bandwidth parameter (close to 1) gives a very smooth curve that may miss the general trend, while a low bandwidth parameter (close to 0) does not smooth the plot much.
// MAGIC
// MAGIC LOESS fit curves are now available for scatter plots. Here is an example of how you can create a LOESS fit for your scatter plots.
// MAGIC
// MAGIC **NOTE:** If your dataset has more than 5000 data points, the LOESS fit is computed using the first 5000 points.

// COMMAND ----------

// Create data points for scatter plot
val rng = new scala.util.Random(0)
val points = sc.parallelize((0L until 1000L).map { x => (x/100.0, 4 * math.sin(x/100.0) + rng.nextGaussian()) }).toDF()

// COMMAND ----------

// MAGIC %md You can turn this data into a scatter plot using the controls on the bottom left of the display table.
// MAGIC
// MAGIC ![plot-menu-pick-scatter](https://docs.databricks.com/_static/images/notebooks/plot-menu-pick-scatter.png)
// MAGIC
// MAGIC You can now access the LOESS fit option when you select *Plot Options*:
// MAGIC
// MAGIC
// MAGIC ![screen shot 2015-10-13 at 3 43 16 pm](https://cloud.githubusercontent.com/assets/7594753/10472058/d7ce763c-71d0-11e5-91b2-4d90e9a704c9.png)
// MAGIC
// MAGIC You can experiment with the bandwith parameter to see how the curve adapts to noisy data.
// MAGIC
// MAGIC Once you accept the change, you will see the LOESS fit on your scatter plot!

// COMMAND ----------

display(points)

// COMMAND ----------

// MAGIC %md ### A **Histogram** allows you to determine the distribution of values.
// MAGIC * **Plot Options...** was selected to configure the graph below.
// MAGIC * **Value** should contain exactly one field.
// MAGIC * **Series Grouping** is always ignored.
// MAGIC * **Keys** can support up to 2 fields.
// MAGIC   * When no key is specified, exactly one histogram is output.
// MAGIC   * When 2 fields are specified, then there is a trellis of histograms.
// MAGIC * **Aggregation** is not applicable.
// MAGIC * **Number of bins** is a special option that appears only for histogram plots, and controls the number of bins in the histogram.
// MAGIC * Bins are computed on the serverside for histograms, so it can plot all the rows in a table.

// COMMAND ----------

// Hover over the entry in the histogram to read off the exact valued plotted.
case class HistogramEntry(key1: String, key2: String, value: Double)
val HistogramRDD = sc.parallelize(
  HistogramEntry("a", "x", 0.2) :: HistogramEntry("a", "x", 0.4) :: HistogramEntry("a", "x", 0.6) :: HistogramEntry("a", "x", 0.8) :: HistogramEntry("a", "x", 1.0) ::
  HistogramEntry("b", "z", 0.2) :: HistogramEntry("b", "x", 0.4) :: HistogramEntry("b", "x", 0.6) :: HistogramEntry("b", "y", 0.8) :: HistogramEntry("b", "x", 1.0) ::
  HistogramEntry("a", "x", 0.2) :: HistogramEntry("a", "y", 0.4) :: HistogramEntry("a", "x", 0.6) :: HistogramEntry("a", "x", 0.8) :: HistogramEntry("a", "x", 1.0) ::
  HistogramEntry("b", "x", 0.2) :: HistogramEntry("b", "x", 0.4) :: HistogramEntry("b", "x", 0.6) :: HistogramEntry("b", "z", 0.8) :: HistogramEntry("b", "x", 1.0) ::
  Nil).toDF()
display(HistogramRDD)

// COMMAND ----------

// MAGIC %md ### A **Quantile plot** allows you to view what the value is for a given quantile value.
// MAGIC * For more information on Quantile Plots, see http://en.wikipedia.org/wiki/Normal_probability_plot.
// MAGIC * **Plot Options...** was selected to configure the graph below.
// MAGIC * **Value** should contain exactly one field.
// MAGIC * **Series Grouping** is always ignored.
// MAGIC * **Keys** can support up to 2 fields.
// MAGIC   * When no key is specified, exactly one quantile plot is output.
// MAGIC   * When 2 fields are specified, then there is a trellis of quantile plots .
// MAGIC * **Aggregation** is not applicable.
// MAGIC * Quantiles are not being calculated on the serverside for now, so only the 1000 rows can be reflected in the plot.

// COMMAND ----------

case class QuantileEntry(key: String, grouping: String, otherField: Int, value: Int)
val quantileSeries = for (x <- 1 to 5000) yield QuantileEntry("key_%01d".format(x % 4),"group_%01d".format(x % 3), x, x*x)
val quantileSeriesRDD = sc.parallelize(quantileSeries).toDF()
display(quantileSeriesRDD)

// COMMAND ----------

// MAGIC %md ### A **Q-Q plot** shows you how a field of values are distributed.
// MAGIC * For more information on Q-Q plots, see http://en.wikipedia.org/wiki/Q%E2%80%93Q_plot.
// MAGIC * **Value** should contain one or two fields.
// MAGIC * **Series Grouping** is always ignored.
// MAGIC * **Keys** can support up to 2 fields.
// MAGIC   * When no key is specified, exactly one quantile plot is output.
// MAGIC   * When 2 fields are specified, then there is a trellis of quantile plots .
// MAGIC * **Aggregation** is not applicable.
// MAGIC * Q-Q Plots are not being calculated on the serverside for now, so only the 1000 rows can be reflected in the plot.

// COMMAND ----------

case class QQPlotEntry(key: String, grouping: String, value: Int, value_squared: Int)
val qqPlotSeries = for (x <- 1 to 5000) yield QQPlotEntry("k_%03d".format(x % 5),"group_%01d".format(x % 3), x, x*x)
val qqPlotRDD = sc.parallelize(qqPlotSeries).toDF()

// COMMAND ----------

// MAGIC %md When there is only one field specified for Values, a Q-Q plot will just compare the distribution of the field with a normal distribution.

// COMMAND ----------

display(qqPlotRDD)

// COMMAND ----------

// MAGIC %md When there are two fields specified for Values, a Q-Q plot will compare the distribution of the two fields with each other.

// COMMAND ----------

display(qqPlotRDD)

// COMMAND ----------

// MAGIC %md Up to two keys can be configured with a Q-Q plot to create a trellis of plots.

// COMMAND ----------

display(qqPlotRDD)

// COMMAND ----------

// MAGIC %md
// MAGIC ### A **Box plot** gives you an idea of what the expected range of values are and shows the outliers.
// MAGIC * See http://en.wikipedia.org/wiki/Box_plot for more information on Box Plots.
// MAGIC * **Value** should contain exactly one field.
// MAGIC * **Series Grouping** is always ignored.
// MAGIC * **Keys** can be added.
// MAGIC   * There will be one box and whisker plot for each combination of values for the keys.
// MAGIC * **Aggregation** is not applicable.
// MAGIC * Box plots are not being calculated on the serverside for now, so only the first 1000 rows can be reflected in the plot.
// MAGIC * The Median value of the Box plot is displayed when you hover over the box.
// MAGIC

// COMMAND ----------

// Hovering over the Box will display the exact median value.
import java.util.Random
case class BoxEntry(key: String, grouping: String, value: Int)
val randomGenerator = new Random()
val boxSeries = for (x <- 1 to 5000) yield BoxEntry("key_%01d".format(x % 2),"group_%01d".format(x % 3), randomGenerator.nextInt(x).toInt)
val boxSeriesRDD = sc.parallelize(boxSeries).toDF()
display(boxSeriesRDD)
