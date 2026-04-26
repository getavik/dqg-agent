import google.generativeai as genai
import pandas as pd
import json
import os
from src.llm_utils import _get_available_model

def detect_pii(df: pd.DataFrame, api_key: str = None, sample_rows: int = 15) -> dict:
    """
    Scans a dataframe for PII/Sensitive data using Google Gemini, 
    guided by DAMA-DMBOK classification standards.
    """
    if not api_key:
        return {"error": "Gemini API Key is required for PII detection."}

    try:
        genai.configure(api_key=api_key)
        model, _ = _get_available_model(api_key=api_key)
        if model is None:
            return {"error": "No suitable Gemini model found that supports generateContent."}
        
        # Prepare a rich technical summary of the data for the LLM
        # We send a sample and some metadata to allow "Cross-Column" reasoning
        sample_data = df.head(sample_rows).to_json(orient="records")
        columns_metadata = {col: str(df[col].dtype) for col in df.columns}
        
        prompt = f"""
        Act as a Data Governance Steward following DAMA-DMBOK standards. Analyze the following dataset sample and metadata to identify PII (Personally Identifiable Information) and Sensitive Data.

        DAMA Classification Framework:
        1. Linked Data (Direct Identifiers): Data that identifies an individual immediately (e.g., SSN, Passport, Full Name).
        2. Linkable Data (Quasi-Identifiers): Data that can identify an individual when combined (the "Mosaic Effect") (e.g., ZIP + Gender + DOB).
        3. Sensitive Data: Strategic or internal data requiring protection (e.g., trade secrets, proprietary logic).

        Six Dimensions of Data Quality (DAMA-DMBoK):
        1. Accuracy: How well does a piece of information reflect the reality it represents?
        2. Completeness: Does the data include all the required information?
        3. Consistency: Is the data consistent within the same dataset and across different datasets?
        4. Timeliness: Is the information available when it is needed?
        5. Uniqueness: Is there only one record of each entity in the dataset?
        6. Validity: Does the data conform to a specific format or standard?

        Dataset Metadata:
        {json.dumps(columns_metadata)}

        Dataset Sample (First {sample_rows} rows):
        {sample_data}

        Task:
        - Identify which columns fall into 'Restricted' (PII/PHI) or 'Confidential' (Internal Sensitive) categories.
        - For each flagged column, specify if it is a 'Direct Identifier' or 'Quasi-Identifier'.
        - Analyze the data in the context of the six dimensions of data quality and identify any potential issues.
        - Distinguish between true PII and technical attributes (e.g., 'refresh_rate' is technical, not a timestamp PII).

        Return ONLY a JSON object where keys are column names and values are lists of detected DAMA categories/entities and data quality issues.
        Example: {{"email": ["PII", "Direct Identifier", "Completeness Issue"], "zip_code": ["PII", "Quasi-Identifier", "Validity Issue"]}}
        """

        response = model.generate_content(prompt)
        clean_text = response.text.strip().replace("```json", "").replace("```", "")
        return json.loads(clean_text)

    except Exception as e:
        return {"error": f"Gemini PII Detection Failed: {str(e)}"}
