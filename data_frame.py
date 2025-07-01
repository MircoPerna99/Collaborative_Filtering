from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import lit
from pyspark.sql.functions import expr
class DataFrame():
    
    def __init__(self, PPIs, drug_taget_interactions):
        self.spark = SparkSession.builder \
                .appName("Collaborative_Filtering") \
                .config("spark.driver.host", "localhost") \
                .config("spark.driver.bindAddress", "127.0.0.1") \
                .getOrCreate()
            
        self.spark.sparkContext.setLogLevel("ERROR")
                
        PPI_list = [PPI.to_list() for PPI in PPIs]
        self.PPIs_df = self.spark.createDataFrame(PPI_list, ["ID_Protein_A", "ID_Protein_B"])
        
        drug_target_interactions_list = [interaction.to_list() for interaction in drug_taget_interactions]
        self.drug_taget_interactions_df = self.spark.createDataFrame(drug_target_interactions_list, ["ID_DrugBank", "ID_Target"])
    
    def show_PPI(self):
        self.PPIs_df.show()
        
    def show_drug_target_interactions(self):
        self.drug_taget_interactions_df.show()
        
    def create_drug_interactions(self):
        self.drug_interactions_df = self.drug_taget_interactions_df.groupBy("ID_DrugBank").agg(collect_list("ID_Target").alias("Proteins")) \
                                                                   .orderBy("ID_DrugBank")
        
        self.drug_interactions_df.show()
    
    def create_date_frame_for_als(self):
        joined_df = self.drug_taget_interactions_df.join(self.PPIs_df, self.drug_taget_interactions_df.ID_Target == self.PPIs_df.ID_Protein_A)
        joined_df.orderBy("ID_DrugBank").show()
        
        joined_df = joined_df.withColumnRenamed("ID_DrugBank", "ID_Drug")
        joined_df.show()
        joined_df = joined_df.join(self.drug_interactions_df, joined_df.ID_Drug == self.drug_interactions_df.ID_DrugBank)
        joined_df = joined_df.select(joined_df["ID_Drug"], joined_df["ID_Target"], joined_df["ID_Protein_A"], joined_df["ID_Protein_B"], joined_df["Proteins"])
        joined_df = joined_df.withColumn("Interaction", lit(1))
        joined_df.show()
        
        joined_df = joined_df.withColumn("Interactor_drug_target", expr("array_contains(Proteins, ID_Protein_B)"))
        joined_df.show()