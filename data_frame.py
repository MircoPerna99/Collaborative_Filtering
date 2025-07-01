from pyspark.sql import SparkSession
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
        
    def show_drug_interactions(self):
        self.drug_taget_interactions_df.show()
        