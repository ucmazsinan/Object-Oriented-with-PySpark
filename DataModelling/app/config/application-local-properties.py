{
'SPARK_VERSION' : '3.3.0',

'USERNAME' : 'postgres',
'PASSWORD' :  b'gAAAAABjUV4lMTS9cmElkCYAeZ8VSincXlWFrd6vaLGO9l2N6M76EqBXESWjNzjwiau6nkoZQ2l3FLLrVLeuML9F0IvD5fkOSQ==',

'HOST' : 'localhost' ,
'PORT' : '5455',
'DBNAME' : 'postgres',

'URL': f'jdbc:postgresql://localhost:5455/postgres',
'JDBC' : r"\postgresql-42.3.1.jar",
'DRIVER' : 'org.postgresql.Driver',
'TRAINTABLE' : 'public.train',

"loadingPath" : "C:\\Users\\user\\Desktop\\pySpark\\df.parquet",
"savingPath"  : "C:\\Users\\user\\Desktop\\data\\",
"fileNames"   : ["hourDf", "fourWeekSalesSum", "ninetyDaysSalesSum", "destinationGroupDf", "serviceTypeGroupDf", "accordingtoHoursGroupDf"]  ,

"BATCH_SCHEDULE" : 15
}

