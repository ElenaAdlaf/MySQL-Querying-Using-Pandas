# MySQL Querying Using Pandas
# Author: Elena Adlaf
# Version 1.1, 10/12/17
# This Python file shows how to query results from table, 't', in database, 'af', stored on a local MySQL server while
# importing the values directly into a Pandas dataframe.

# The table lists details about pieces created by the custom furniture business, Artfully Functional,
# with fields for ID, size type, year built, labor hours, materials cost, sale prices (wholesale or retail,
# before or after sales tax) and potential profits. A second table, 'a', contains additional information and is
# used to demonstrate queries indexing or joining multiple tables.


# Import modules.
import mysql.connector
import pandas as pd

# Create variables for 1) a connector to the local database with user and password and 2) the read-to-pandas command
cnx = mysql.connector.connect(user='root', password='...', database='af')
g = pd.read_sql_query

# To import the entire table, 't', into a Pandas dataframe:
df = g('SELECT * FROM t', cnx)

# Look at the shape of the dataframe and index the first five records for all of the fields.
print(df.shape)
print(df.iloc[0:5, 0:14])
print(df.iloc[0:5, 14:])


# Most tables will likely be too large to import in full, so we can import only the data of interest by
# querying the database through Pandas.

# Return the column names and column info of the table, 't'.
col_names = g('SHOW COLUMNS FROM t', cnx)
print(col_names)

# Select only Name and Retail_High columns and limit the number of records returned.
namehighretail_firstten = g('SELECT Name, Retail_High FROM t LIMIT 10', cnx)
print(namehighretail_firstten)

# Select all unique values from the Yr column.
years = g('SELECT DISTINCT Yr FROM t', cnx)
print(years)

# Return the number of records in the table.
num_tablerows = g('SELECT COUNT(*) FROM t', cnx)
print(num_tablerows)

# Return the number of non-missing values in the Labor column.
num_laborvalues = g('SELECT COUNT(Labor) FROM t', cnx)
print(num_laborvalues)

# Return the number of distinct values in Yr column.
num_years = g('SELECT COUNT(DISTINCT Yr) FROM t', cnx)
print(num_years)

# Select names of all pieces with a Retail_Low value greater than or equal to $500
over500usd = g('SELECT Name FROM t WHERE Retail_Low >= 500', cnx)
print(over500usd)

# Select the ID number of all pieces whose Sale value is null.
idprofitnull = g('SELECT ID FROM t WHERE Sale IS NULL', cnx)
print(idprofitnull)

# Return the number of items whose build year is not 2017.
num_not2017 = g('SELECT COUNT(*) FROM t WHERE Yr <> 2017', cnx)
print(num_not2017)

# Select name and location (disposition) of items with a low retail price over 100 or a low wholesale price over 50.
nameloc_price = g('SELECT Name, Disposition FROM t WHERE Retail_Low > 100 OR Wholesale_Low > 50', cnx)
print(nameloc_price)

# Select the labor hours of items built in 2015 or 2017 and located at Holloway or Art Show
laborhours_notforsale = g("SELECT Labor FROM t WHERE (Yr = 2015 OR Yr = 2017) AND (Disposition = 'Holloway' OR "
                      "Disposition = 'Art Show')", cnx)
print(laborhours_notforsale)

# Select the class of items whose potential profit (retail high) is between 10 and 50.
class_ptlprofit = g('SELECT Class_version FROM t WHERE Ptnlprofit_rtl_High BETWEEN 10 AND 50', cnx)
print(class_ptlprofit)

# Select the disposition, class, and potential high wholesale profit for the items with disposition as Classic Tres,
# Art Show or For Sale. Calculate the sum of the returned potential profits.
ptlprofit_forsale = g("SELECT Disposition, Class_version, Ptnlprofit_whsle_High FROM t WHERE Disposition IN "
                      "('Classic Tres', 'Art Show', 'For Sale') AND Ptnlprofit_whsle_High > 0", cnx)
print(ptlprofit_forsale)
print(ptlprofit_forsale.sum(axis=0, numeric_only=True))

# Select the ID, name and class_version designation of all C-class items.
c_class_items = g("SELECT ID, Name, Class_version FROM t WHERE Class_version LIKE 'C%'", cnx)
print(c_class_items)

# Select name and retail prices of all tables. Calculate the lowest and highest table prices.
tables_retail = g("SELECT Name, Retail_Low, Retail_High FROM t WHERE Name LIKE '% Table' AND Retail_Low <> 0", cnx)
print(tables_retail)
print(tables_retail.agg({'Retail_Low' : ['min'], 'Retail_High' : ['max']}))

# Select names and labor hours of tables that don't include side tables.
noside = g("SELECT Name, Labor FROM t WHERE Name LIKE '% Table' AND Name NOT LIKE '%_ide %'", cnx)
print(noside)

