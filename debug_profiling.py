
import pandas as pd
from ydata_profiling import ProfileReport
import sys

try:
    df = pd.read_csv("sample_data.csv")
    profile = ProfileReport(df, minimal=True)
    description = profile.get_description()
    
    print(f"Type of description: {type(description)}")
    print(f"Attributes: {dir(description)}")
    
    # Check if we can access attributes
    if hasattr(description, 'table'):
        print("Has 'table' attribute")
        print(f"Table type: {type(description.table)}")
        print(f"Table attributes: {dir(description.table)}")
    
    if hasattr(description, 'variables'):
         print("Has 'variables' attribute")
except Exception as e:
    print(e)
