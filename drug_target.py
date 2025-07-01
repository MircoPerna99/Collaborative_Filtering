class DrugTarget():
    def __init__(self):
        self.drug = ""
        self.target = ""
        
    def set_properties(self, drug, target):
        self.drug = drug
        self.target = target
        
    def to_list(self):
        return [self.drug, self.target]