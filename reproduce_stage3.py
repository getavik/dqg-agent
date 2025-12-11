
import pandas as pd
import great_expectations as gx
import sys

try:
    print(f"Great Expectations version: {gx.__version__}")
    
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    context = gx.get_context()
    
    datasource_name = "test_datasource"
    asset_name = "test_asset"
    
    print("Attempting to add pandas datasource and dataframe asset...")
    # This is the line from src/validator.py
    try:
        batch_definition = context.data_sources.add_pandas(datasource_name).add_dataframe_asset(name=asset_name, dataframe=df)
        print("Success with dataframe argument!")
    except TypeError as e:
        print(f"Caught expected TypeError: {e}")
        
    print("Attempting fix (removing dataframe argument)...")
    try:
        # Proposed fix
        datasource = context.data_sources.add_pandas(datasource_name + "_fixed")
        asset = datasource.add_dataframe_asset(name=asset_name)
        print("Success without dataframe argument!")
        
        # Verify we can get a batch
        # Note: add_batch_definition might be needed depending on version, checking basic asset creation first
        # In the original code it was chained: .add_batch_definition(name="my_batch_def")
    except Exception as e:
        print(f"Fix failed with: {e}")

except Exception as e:
    print(f"General error: {e}")
    import traceback
    traceback.print_exc()
