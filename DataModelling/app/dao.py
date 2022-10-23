from app.utils import Allocate, Format
from app.config.config import config

class Load(Allocate,Format):

    """
    retrive data from local or server where data stored inside Pyspark for analyzes

 

    """


    def __init__(self,spark):
        self.spark = spark


    def load_table_names(self):
        """
        colNames:
        rowNames:
        """
        dbInfoDf = self.load_from_database(dbtable = 'pg_catalog.pg_tables')

        # extract database names and table names from database 

        tableNamesDf = self.allocate_by_rows(dbInfoDf,"schemaname", ["public"]) 

        # gathering specify table names and db names 
        schemaNamesDf = self.allocate_by_columns(tableNamesDf,["schemaname","tablename"])

        # merge databases names and table names like that public.demand and finaly return a list     
        tableNameList = self.format_table_names(schemaNamesDf)   

        return tableNameList

    
    def load_file(self, fileType = 'parquet'):
        """
        Parameters:

        fileType : your datas which type you must specified in here ex : .parquet, .csv, .txt what you want any type of file 

        """
        df = self.spark.read.format(fileType).load("{0}".format(config["loadingPath"]))
        
        return df 

        
    def load_multiple_files(self, dfList):

        """
            if you have an list of dataframe you can extract them multiple into local or server  
            inside loop take an two list one of dfname other list of df inside zip and return a one by one as an dicitonary format.

            Paramaters 

            dfList : list of dataframe

            

        """

        dfDictionary = {}

        for df,fileName in zip(dfList,config["fileNames"]):
            df = self.load_file(df = df, fileName = fileName)
            dfDictionary[fileName] = df 

        return dfDictionary
    
    def load_from_database(self,dbtable):
        df = self.spark.read.format('jdbc')\
        .options(
         url=config["URL"], # database url (local, remote)
         dbtable=dbtable,
         user=config["USERNAME"],
         password=config["PASSWORD"],
         driver=config["DRIVER"])\
        .load()
        return df
         
    def load_streaming_data_frame(self):
        pass



class Save(Load):

    """
    Extract data into new files from dataframe given format type and file name 
    or taking from data inside pyspark and read them 
    """

    def save_file(self, df, fileName, fileType = 'parquet'):

        """

        Paramaters

        df : refer dataframe 

        fileName : specific new file name according to purpose of data

        fileType : extract from data given file type ex : .parquet, .csv, .txt what you want any type of file 
        """
        #df.write.save(f"{savingPath}{fileName}.{fileType}")
        df.toPandas().to_parquet("{0}{1}.{2}".format(config["savingPath"],fileName,fileType))
    
    def save_multiple_files(self, dfList):

        """
            if you have an list of dataframe you can extract them multiple into local or server  
            inside loop take an two list one of dfname other list of df inside zip and return a one by one 

            Paramaters 

            dfList : list of dataframe

            fileNames : list of filenames according to data 

        """

        for df,fileName in zip(dfList,config["fileNames"]):

            self.save_file(df = df, fileName = fileName)
    
    def save_into_database(self,df, tableName, mode = 'error'): 

        df.write.format("jdbc").options(url = config["URL"], 
        dbtable=tableName, 
        user=config["USERNAME"], 
        password=config["PASSWORD"], 
        driver=config["DRIVER"]).mode(mode).save()

    def save_tables_into_database(self,dfList):

        tableNamesList = self.load_table_names()
        for tableName,df in zip(config["fileNames"],dfList):

            if tableName in tableNamesList:
                self.save_into_database(df, tableName, mode = 'overwrite')
            elif tableName not in tableNamesList:
                self.save_into_database(df, tableName, mode = 'error')



