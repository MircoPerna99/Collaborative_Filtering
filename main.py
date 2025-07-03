from data_access import DataAccess
from data_frame import DataFrame
from als_model import ALSModel
from pyspark.sql.functions import first
_data_access = DataAccess()

ppis = _data_access.get_PPIs()
drug_target_interactions = _data_access.get_drug_target_interactions()
_data_access.close_connection()

dataFrame = DataFrame(ppis, drug_target_interactions)
dataFrame.create_date_frame_for_als()

model = ALSModel(dataFrame.joined_df)
model.calculate_recommended_proteins()
dataFrame.joined_df.orderBy("ID_Drug").groupBy("ID_Drug").pivot("ID_Protein").agg(first("Interactions")).show()
model.drug_proteins_recommended.orderBy("ID_Drug").groupBy("ID_Drug").pivot("ID_Protein").agg(first("rating")).show()
