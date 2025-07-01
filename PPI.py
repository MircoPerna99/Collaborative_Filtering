class PPI():
    def __init__(self):
        self.protein_A = ""
        self.protein_B = ""
        
    def set_properties(self, protein_A, protein_B):
        self.protein_A = protein_A
        self.protein_B = protein_B
    
    def to_list(self):
        return [self.protein_A, self.protein_B]