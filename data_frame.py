from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import lit
from pyspark.sql.functions import expr
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline

class DataFrame():
    
    def __init__(self, PPIs, drug_taget_interactions):
        self.spark = SparkSession.builder \
                .appName("Collaborative_Filtering") \
                .config("spark.driver.host", "localhost") \
                .config("spark.driver.bindAddress", "127.0.0.1") \
                .getOrCreate()
            
        self.spark.sparkContext.setLogLevel("ERROR")
                
        self.create_ppi_dataframe(PPIs)
        
        self.create_drug_taget_interactions_dataframe(drug_taget_interactions)

    
    def create_ppi_dataframe(self,PPIs):
        PPI_list = [PPI.to_list() for PPI in PPIs]
        self.PPIs_df = self.spark.createDataFrame(PPI_list, ["ID_Protein_A", "ID_Protein_B"])
    
    def create_drug_taget_interactions_dataframe(self,drug_taget_interactions):
        drug_target_interactions_list = [interaction.to_list() for interaction in drug_taget_interactions]
        self.drug_taget_interactions_df = self.spark.createDataFrame(drug_target_interactions_list, ["ID_DrugBank", "ID_Target"])
    
    def show_PPI(self):
        self.PPIs_df.show()
        
    def show_drug_target_interactions(self):
        self.drug_taget_interactions_df.show()

    def create_date_frame_for_als(self):
        def create_drug_interactions():
            self.drug_interactions_df = self.drug_taget_interactions_df.groupBy("ID_DrugBank") \
                                                                    .agg(collect_list("ID_Target") \
                                                                    .alias("Proteins")) \
                                                                    .orderBy("ID_DrugBank")
            self.drug_interactions_df.show() 
        
        def join_drug_target_with_ppi():
            self.joined_df = self.drug_taget_interactions_df.join(self.PPIs_df, self.drug_taget_interactions_df.ID_Target == self.PPIs_df.ID_Protein_A)            
            self.joined_df = self.joined_df.withColumnRenamed("ID_DrugBank", "ID_Drug")
            self.joined_df.show()
        
        def filter_interactions():
            self.joined_df = self.joined_df.join(self.drug_interactions_df, self.joined_df.ID_Drug == self.drug_interactions_df.ID_DrugBank)
            self.joined_df = self.joined_df.select(self.joined_df["ID_Drug"], self.joined_df["ID_Target"], self.joined_df["ID_Protein_A"], self.joined_df["ID_Protein_B"], self.joined_df["Proteins"])
            self.joined_df.show()
            
            self.joined_df = self.joined_df.withColumn("Interactor_drug_target", expr("array_contains(Proteins, ID_Protein_B)"))
            self.joined_df.show()
            
            self.joined_df = self.joined_df.filter(self.joined_df.Interactor_drug_target == False)
            
        def calculate_interaction():
            columns_to_group_by = ["ID_Drug", "ID_Protein_B"]
            self.joined_df = self.joined_df.groupBy(columns_to_group_by).count().alias("Interactions")
            self.joined_df.show()
            
        def indexing_id():
            drug_indexer = StringIndexer(inputCol = "ID_Drug", outputCol = "ID_Drug_Index").fit(self.joined_df)
            prontein_indexer = StringIndexer(inputCol = "ID_Protein_B", outputCol = "ID_Protein_Index").fit(self.joined_df)
            pipeline = Pipeline(stages = [drug_indexer, prontein_indexer])
            self.indexed_df = pipeline.fit(self.joined_df).transform(self.joined_df)
            self.indexed_df.show()
            
        create_drug_interactions()
        
        join_drug_target_with_ppi()
        
        filter_interactions()
        
        calculate_interaction()
        
        indexing_id()

        