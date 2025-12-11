
import pandas as pd
import sys
import os

# Ensure src is in path
sys.path.append(os.path.abspath("."))

try:
    from src.profiler import generate_profile
except ImportError as e:
    print(f"Import Error: {e}")
    sys.exit(1)

try:
    print("Loading sample data...")
    df = pd.read_csv("sample_data.csv")
    print(f"Data loaded: {df.shape}")
    
    print("Running generate_profile...")
    result = generate_profile(df)
    print("Profile generated successfully.")
    print("Keys in result:", result.keys())
    
except Exception as e:
    print(f"Caught exception in reproduction script:")
    print(e)
    import traceback
    traceback.print_exc()
