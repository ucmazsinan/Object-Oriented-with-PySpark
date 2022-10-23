from app.utils import Create, Assemble

from pyspark.sql.functions import min, max, mean, avg, count,sum
from pyspark.sql.functions import col, lit, when,filter, window, concat
from pyspark.sql.functions import year, month, weekofyear,dayofweek, hour,dayofmonth,days



class Analyze(Create,Assemble):
    """
        Analyze class inherit from Create class and Create class inherit from Process class


    """

    def  __init__(self, spark):
        super().__init__(spark) 


    def analyze_hour(self, df, baseVariable = 'Saat', targetVariable = 'Satış', pairs = [(3,7), (8,12), (15,19)], measure = sum):
        """
        create a new  hours dataframe given hours pairs and variables:
        analyze_hour(self, df, baseVariable, targetVariable, pair, measure)

        Parameters:
        
        df : refer the dataframe 

        baseVariable : refer the hours column if does not exist extract from timestamp column 
        
        targetVariable : refers the quantitive column
        
        pairs : refer the each value between values  as an tuple  ex: (5,6)
        
        measure : refer the aggregation functions mean, max etc
        """
        

        # using process pairs method from Process class  to retrieve hour pairs 


        hourData = [self.process_pairs(df, baseVariable, targetVariable, pair, measure) for pair in pairs]
        
        columns = [baseVariable, targetVariable]
        
        hourDf = self.create_df(hourData,columns)
        hourDfList = [hourDf]
        return hourDfList 
    
    def analyze_window(self,df):

        """
        return a new dataframe groups given variables 
        self.process_windows(df, targetVariable = 'Satış', targetTimeStamp = 'Tarih',\
                            windowInterval = '4 week', slideInterval = '4 week', measure = sum)

        Parameters:

        df : refers the dataframe 

        targerVariable : quantitive variables measured demands , revenue variables

        targetTimeStamp : specific timestamp variables 
        
        windowInterval : difference between period start and end  7 days, 14 days, 6 week 
        
        slideInterval : time frequencies ex: 7 days, 14 days, 6 week 

        measure : refer the aggregation function
        """
        # resampling from data given the time interval and grouping data according to  quantitive variables

        fourWeekSalesSum = self.process_windows(df, targetVariable = 'Satış', targetTimeStamp = 'Tarih',\
                            windowInterval = '4 week', slideInterval = '4 week', measure = sum)
        
        ninetyDaysSalesSum = self.process_windows(df, targetVariable = 'Satış', targetTimeStamp = 'Tarih',\
                            windowInterval = '90 days', slideInterval = '90 days', measure = sum)
        
        windowsList = [fourWeekSalesSum, ninetyDaysSalesSum]
        
        return windowsList
    
    def analyze_data_groups(self,df):

        """
         using inside from two Class Assemble and Create and Process class 
         assemble class create a new column automaticaly given the roundtrip column 
         process_data_group function create a new group from given variables ex: servicesType passengerclass etc

        """
        
        # Create new column from two column about round trip then second group by columns according to given variable
        
        #df = self.assemble_two_columns(df, 'Hat', 'Gidiş', 'Dönüş')
        destinationGroupDf = self.process_data_group(df, ['Hat', 'Sınıf'], 'Satış' )
        
        serviceTypeGroupDf = self.process_data_group(df, ['ServisTipi','Sınıf'], 'Satış')
        accordingtoHoursGroupDf = self.process_data_group(df, ['Saat','Sınıf'], 'Satış')
        
        groupList = [destinationGroupDf,serviceTypeGroupDf,accordingtoHoursGroupDf]
        
        return groupList
    
    def analyze(self,df):
        """
        Utilizes other method of the class gather into one 
                
        """
        # extract hours from timestamp columns
        df = self.create_time_column(df, 'Saat', 'Tarih', times = hour)

        # apply all analyzes 

        hourDfList = self.analyze_hour(df)
        windowsList = self.analyze_window(df)
        groupList = self.analyze_data_groups(df)

        # concat all list of spark dataframes
        analyzeList = hourDfList + windowsList + groupList

        return analyzeList
        