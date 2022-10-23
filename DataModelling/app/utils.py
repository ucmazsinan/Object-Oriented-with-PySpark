from pyspark.sql.functions import min, max, mean, avg, count,sum
from pyspark.sql.functions import col, lit, when,filter, window, concat
from pyspark.sql.functions import year, month, weekofyear,dayofweek, hour,dayofmonth,days
from app.config.config import config


class Allocate:

    """
    Allocate class gathering database schemas information db names, table name inside database and
    filtering them specifying information about db and table names

    """

    def allocate_by_rows(self,df,colName, rowNames):

        """
        Filter schemas specifying information extract database names and table names 

        Paramaters:

        df : refer the spark dataframe load from database schemas information
        colName : refer the schemanames extract them where database names 
        rowNames : is a list refer the table names or one object table 
        """

        allocatedDf = df.filter(col(colName).isin(rowNames))
        return allocatedDf

    def allocate_by_columns(self,df, colNames):

        """
        select column names from database info dataframe 

        Paramaters:

        df : refer the spark dataframe load from database schemas information
        colNames : is a list  refer the schemanames and table names  extract them where database
        """

        allocatedDf = df.select(colNames)
        return allocatedDf


class Format:

    """
    gather into one list specifying table and schemas names from the spark dataframe is a database info tables
    """

    def format_table_names(self,df):

        """
        return the list inside schemanames and table names like that db.tables 

        Paramaters:

        df : refer the spark dataframe load from database schemas information
        """

        collection = df.collect()
        tableNamesList = [data[0]+"."+data[1]  for data in collection]
        return tableNamesList
        


class Assemble:
     
    """
    Assemble class merged two column into one column here we aim concat roundtrip column 
    but you can give any two string column.   
    """

    def assemble_two_columns(self, df, newColumnNames, targetVariableOne, targetVariableSecond):

        """
        concat two columns into one 

        Paramaters:

        newColumnNames : new column names from two columns names so ı meant common column names

        targetVariableOne : which string column include a dataframes this first parameters 

        targetVariableSecond : which string column include a dataframes this second parameters for merged first 
        """
        assembleDf = df.withColumn(newColumnNames, concat(col(targetVariableOne), lit('-'), col(targetVariableSecond)))
        return assembleDf

class Process:
    def process_data_group(self, df, columnNames, targetVariable, measure = mean):
        """
        process data group by the specified metrics 
        
        
            Parameters:
            
            df : Dataframes 
            columnNames : columnames for the groupby  this can be a list 
            targetVariable : using  the groupby process variable name measure something 
            measure : refer these (mean, max, sum )
            
        """
        groupedData = df.groupBy(columnNames).agg(measure(targetVariable))
        return groupedData
    
    def process_pairs(self, df, baseVariable, targetVariable, pairs, measure = mean):
        """
        process filter data by hour pairs 
        
        Parameters:
        
        df : dataframes
        
        baseVariable : refer the hours column if does not exist extract from timestamp column 
        
        targetVariable : refers the quantitive column sales, demand, revenue 
        
        pairs : refer the each value between values of tuple  ex: (5,6), (7,9), (10,13) 
        
        measure : refer the aggregation functions mean, max etc
        
        """
        filterPairDf = df.filter((col(baseVariable) >= pairs[0]) & (col(baseVariable) <= pairs[1])).\
        select(targetVariable).groupBy().sum()
        
        label = f"{pairs[0]}-{pairs[1]}"
        value = filterPairDf.take(1)[0][0]
        
        return label,value
    
    
    def process_windows(self, df, targetVariable, targetTimeStamp, windowInterval = '4 week', slideInterval = '4 week', measure = sum):
        """
        process data for time series interval ex: python resampling 
        
        Parameters:
        
        df : refer data frames 
        
        targetVariable : refer to the column purpose analysis (price, demand, revenue etc)
        
        targetTimeStamp : refer the timestamp data 
        
        windowInterval : difference between period start and end  7 days, 14 days, 6 week 
        
        slideInterval : time frequencies ex: 7 days, 14 days, 6 week 
        
        measure : refer these (mean, max, sum )
        
        
        """
        
        measureName = measure.__name__ # extract parameter name for naming column
        # step:1 grouping the data using window function parameters 
        # step:2 aggregation funciton ex: sum, mean
        # step:3 select column inside from json type
        # step:4 sorting the data by start date 
        
        step1 = df.groupby(
        window(targetTimeStamp, windowDuration = windowInterval, slideDuration = slideInterval))
        
        step2 = step1.agg(measure(targetVariable))
        
        step3 = step2.select("window.start", "window.end", f"{measureName}({targetVariable})")
        
        windowData = step3.orderBy(col("window.start").asc()) 
        
        return windowData
    
    def process_column_name(self,df, oldColumnName, newColumnName):
   
        """
            renaming old column names 
            
            Parameters: 
            
            df: spark dataframe refer the 

            oldColumnNames : list
                giving old  columns names together using with  list
            
            newColumnNames : list 
                giving  new columns names together using with  list

        """
        
        mapping = dict(zip(oldColumnName,newColumnName))
        
        renameDf = df.select([col(c).alias(mapping.get(c, c)) for c in df.columns])
        
        return renameDf


    
class Create(Process,Assemble):

    def __init__(self, spark):
        self.spark = spark



    def create_time_column(self, df, timeColumnName, baseColumnName, times = hour):
        """
            df : spark dataframes datasets
            timeColumnName : creates new time column names
            baseColumnName : existing timestamp column 
            cast : refer the time, hour, week refer the pspark time 
        """
        newDf = df.withColumn(timeColumnName,times(col(baseColumnName)))
        return newDf
    
    def create_df(self, data, columns):
        
        """
        create a new spark data frame from some aggregation operations taking some hour pairs ex [(3,5), (7,12)]
        
        Paramaters:
        
        df : refer Spark data frame 
        
        data: list of tuple

        columns: list of columnnames 
        """
        
        
        
        df = self.spark.createDataFrame(data, columns)
        
        return df




    
    
    
    
    
        
    