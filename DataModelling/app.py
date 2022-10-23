from app import CreateApp
from app.service import Analyze
from app.dao import Load, Save 


create_app = CreateApp()
spark = create_app.create_app()

print("Loading...")

load = Load(spark)
df = load.load_file()


print("Analyzing...")

analyze = Analyze(spark)
dfList = analyze.analyze(df)

print("Saving...")

save = Save(spark)
#save.save_multiple_files(dfList)
save.save_tables_into_database(dfList)
print("end")
