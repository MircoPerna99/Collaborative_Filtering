from data_access import DataAccess
from data_frame import DataFrame
_data_access = DataAccess()

ppis = _data_access.get_PPIs()
drug_target_interactions = _data_access.get_drug_target_interactions()

_data_access.close_connection()

dataFrame = DataFrame(ppis, drug_target_interactions)
dataFrame.show_PPI()
dataFrame.show_drug_target_interactions()
dataFrame.create_drug_interactions()
dataFrame.create_date_frame_for_als()
