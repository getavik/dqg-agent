from presidio_analyzer import AnalyzerEngine
import pandas as pd

def detect_pii(df: pd.DataFrame, sample_rows: int = 20) -> dict:
    """
    Scans a dataframe for PII using Microsoft Presidio.
    Returns a dictionary of detected entities per column.
    """
    analyzer = AnalyzerEngine()
    pii_results = {}
    
    # Analyze a sample of data to avoid performance issues
    sample_df = df.head(sample_rows)
    
    for col in sample_df.columns:
        # Convert column to string for analysis
        col_values = sample_df[col].astype(str).tolist()
        
        # Aggregate text for analysis (simple approach)
        # In a real scenario, we might analyze row by row or batch
        text_blob = " ".join(col_values)
        
        results = analyzer.analyze(text=text_blob, language='en')
        
        if results:
            entities = list(set([res.entity_type for res in results]))
            pii_results[col] = entities
            
    return pii_results
