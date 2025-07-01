from pymongo import MongoClient
from PPI import PPI
from drug_target import DrugTarget

class DataAccess():
    def __init__(self):
        self.client = MongoClient("mongodb://localhost:27017/")
        self.database = self.client["Biological_Network"]
        self.collection = self.database["Collaborative_Filtering"]
        self.id_collection = "collaborative_filtering"
        

    def get_PPIs(self):
        data = self.collection.find_one({"_id":self.id_collection})
        
        PPIs = []
        for interaction in data["PPI"]:
            new_ppi = PPI()
            new_ppi.set_properties(interaction["ID_Protein_A"], interaction["ID_Protein_B"])
            PPIs.append(new_ppi)
        
        return PPIs
    
    def get_drug_target_interactions(self):
        data = self.collection.find_one({"_id":self.id_collection})
        
        drug_target_interactions = []
        for interaction in data["Drug_Target"]:
            new_drug_target = DrugTarget()
            new_drug_target.set_properties(interaction["ID_DrugBank"], interaction["ID_Target"])
            drug_target_interactions.append(new_drug_target)
        
        return drug_target_interactions
    
    def close_connection(self):
        self.client.close()