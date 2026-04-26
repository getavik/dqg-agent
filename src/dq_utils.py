import pandas as pd
import re
import phonenumbers
import validators
from stdnum.us import ssn as us_ssn

def validate_and_tag_data(df: pd.DataFrame, rules: list) -> pd.DataFrame:
    """
    Validates data against a set of rules and tags each row with its DQ status.
    """
    # Create a copy to avoid modifying the original dataframe in session state
    val_df = df.copy()
    val_df['dq_status'] = 'good'
    # We use a temporary column to track if a row has unfixed issues
    val_df['_issues'] = [[] for _ in range(len(val_df))]
    
    for rule in rules:
        col = rule["column"]
        expectation = rule["expectation"]
        
        if col not in val_df.columns:
            continue

        if expectation == "expect_column_values_to_not_be_null":
            mask_bad = val_df[col].isnull() | (val_df[col].astype(str).str.strip() == "")
            for idx in val_df[mask_bad].index:
                val_df.at[idx, '_issues'].append(f"{col} (null)")
            
        elif expectation == "expect_column_values_to_match_regex":
            regex = rule.get("regex", ".*")
            mask_not_match = ~val_df[col].astype(str).str.match(regex, na=False)
            for idx in val_df[mask_not_match].index:
                val_df.at[idx, '_issues'].append(f"{col} (invalid format)")
            
    # Initial status
    for idx, row in val_df.iterrows():
        if row['_issues']:
            val_df.at[idx, 'dq_status'] = f"bad: {', '.join(row['_issues'])}"
            
    return val_df

def remediate_data(df: pd.DataFrame, rules: list, pii_results: dict = None) -> pd.DataFrame:
    """
    Applies rule-driven, library-powered remediation to the dataframe.
    """
    remediated_df = df.copy()
    pii_results = pii_results or {}
    
    # 1. Apply Fixes
    for rule in rules:
        col = rule["column"]
        expectation = rule["expectation"]
        
        if col not in remediated_df.columns:
            continue

        col_pii = pii_results.get(col, [])
        is_phone = "phone" in col.lower() or any("phone" in str(p).lower() for p in col_pii)
        is_email = "email" in col.lower() or any("email" in str(p).lower() for p in col_pii)
        is_ssn = "ssn" in col.lower() or any("ssn" in str(p).lower() for p in col_pii)
            
        if expectation == "expect_column_values_to_not_be_null":
            mask_blank = remediated_df[col].isnull() | (remediated_df[col].astype(str).str.strip() == "")
            if mask_blank.any():
                if is_email:
                    remediated_df.loc[mask_blank, col] = "placeholder: john.doe@example.com"
                elif is_phone:
                    remediated_df.loc[mask_blank, col] = "placeholder: +1-000-000-0000"
                elif is_ssn:
                    remediated_df.loc[mask_blank, col] = "placeholder: 000-00-0000"
                elif pd.api.types.is_numeric_dtype(remediated_df[col]):
                    remediated_df.loc[mask_blank, col] = 0
                else:
                    remediated_df.loc[mask_blank, col] = "Unknown"
            
        elif expectation == "expect_column_values_to_match_regex" or True: # Apply library standardization even if no explicit regex rule if it's a known type
            
            if is_phone:
                def standardize_phone(val):
                    val_str = str(val).strip()
                    if pd.isna(val) or val_str == "" or val_str.lower() in ["unknown", "invalid phone", "nan", "none"]:
                        return f"placeholder: +1-000-000-0000"
                    try:
                        parsed = phonenumbers.parse(val_str, "US")
                        if phonenumbers.is_valid_number(parsed):
                            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.INTERNATIONAL).replace(" ", "-")
                        return f"placeholder: +1-000-000-0000"
                    except:
                        return f"placeholder: +1-000-000-0000"
                remediated_df[col] = remediated_df[col].apply(standardize_phone)
            
            elif is_email:
                def standardize_email(val):
                    val_str = str(val).strip()
                    if pd.isna(val) or val_str == "" or not validators.email(val_str):
                        return "placeholder: john.doe@example.com"
                    return val_str
                remediated_df[col] = remediated_df[col].apply(standardize_email)

            elif is_ssn:
                def standardize_ssn(val):
                    val_str = str(val).strip()
                    if pd.isna(val) or val_str == "":
                        return "placeholder: 000-00-0000"
                    # Try to cleanse using stdnum
                    try:
                        if us_ssn.is_valid(val_str):
                            return us_ssn.format(val_str)
                        # Attempt to fix by stripping non-digits
                        compact = us_ssn.compact(val_str)
                        if len(compact) == 9:
                            return us_ssn.format(compact)
                        return "placeholder: 000-00-0000"
                    except:
                        return "placeholder: 000-00-0000"
                remediated_df[col] = remediated_df[col].apply(standardize_ssn)

    # 2. Update Status and Verify Fixes
    original_df = df
    for index, row in remediated_df.iterrows():
        fixed_issues = []
        remaining_issues = []
        needs_manual_review = False
        
        # Check all rules again to see what is still broken
        for rule in rules:
            col = rule["column"]
            if col not in remediated_df.columns: continue
            
            is_currently_broken = False
            issue_desc = ""
            if rule["expectation"] == "expect_column_values_to_not_be_null":
                if pd.isna(row[col]) or str(row[col]).strip() == "":
                    is_currently_broken = True
                    issue_desc = f"{col} (null)"
            elif rule["expectation"] == "expect_column_values_to_match_regex":
                regex = rule.get("regex", ".*")
                if not re.match(regex, str(row[col])):
                    is_currently_broken = True
                    issue_desc = f"{col} (invalid format)"
            
            if is_currently_broken:
                remaining_issues.append(issue_desc)
        
        # Check what was fixed compared to original
        for col in remediated_df.columns:
            if col in ['dq_status', '_issues']: continue
            orig_val = original_df.loc[index, col]
            new_val = row[col]
            
            # Robust comparison
            def are_values_equivalent(v1, v2):
                if pd.isna(v1) and pd.isna(v2): return True
                if pd.isna(v1) or pd.isna(v2): return False
                try:
                    if str(v1).strip() == str(v2).strip(): return True
                except: pass
                return v1 == v2

            if not are_values_equivalent(orig_val, new_val):
                if str(new_val).startswith("placeholder:"):
                    placeholder_val = str(new_val).replace("placeholder: ", "")
                    remediated_df.loc[index, col] = placeholder_val
                    fixed_issues.append(f"{col} (fixed with placeholder: {placeholder_val})")
                    needs_manual_review = True
                else:
                    fixed_issues.append(f"{col} (standardized: {orig_val} -> {new_val})")

        # Final Status Construction
        if not remaining_issues and not fixed_issues:
            remediated_df.at[index, 'dq_status'] = 'good'
        elif not remaining_issues and fixed_issues:
            status = "fixed"
            if needs_manual_review: status = "fixed (needs manual update)"
            remediated_df.at[index, 'dq_status'] = f"{status}: {', '.join(fixed_issues)}"
        elif remaining_issues and fixed_issues:
            status = "partially fixed"
            if needs_manual_review: status = "partially fixed (needs manual update)"
            remediated_df.at[index, 'dq_status'] = f"{status}: fixed {', '.join(fixed_issues)}; still bad {', '.join(remaining_issues)}"
        else:
            remediated_df.at[index, 'dq_status'] = f"bad: {', '.join(remaining_issues)}"

    if '_issues' in remediated_df.columns:
        remediated_df.drop(columns=['_issues'], inplace=True)
            
    return remediated_df


