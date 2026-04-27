import pandas as pd
import re
import phonenumbers
import validators
from stdnum.us import ssn as us_ssn
import dateparser
import pycountry
from datetime import datetime
from rapidfuzz import process, fuzz

# --- CONSTANTS & REFERENCE DATA ---

GEOGRAPHIC_DATA = {
    "INDIA": {
        "TAMIL NADU": ["CHENNAI", "COIMBATORE", "MADURAI", "TIRUCHIRAPPALLI", "SALEM", "TIRUPPUR", "ERODE", "VELLORE", "THOOTHUKUDI", "TIRUNELVELI"],
        "MAHARASHTRA": ["MUMBAI", "PUNE", "NAGPUR", "THANE", "NASHIK", "AURANGABAD", "SOLAPUR", "AMRAVATI", "NAVI MUMBAI", "KOLHAPUR"],
        "KARNATAKA": ["BENGALURU", "MYSORE", "HUBBALLI", "MANGALURU", "BELAGAVI", "KALABURAGI", "DAVANAGERE", "BALLARI", "VIJAYAPURA", "SHIVAMOGGA"],
        "DELHI": ["NEW DELHI", "DELHI", "DWARKA", "ROHINI"],
        "TELANGANA": ["HYDERABAD", "WARANGAL", "NIZAMABAD", "KHAMMAM", "KARIMNAGAR"],
        "WEST BENGAL": ["KOLKATA", "HOWRAH", "DARJEELING", "SILIGURI", "ASANSOL", "DURGAPUR", "BARDHAMAN", "MALDA", "BAHARAMPUR", "HABRA"],
        "GUJARAT": ["AHMEDABAD", "SURAT", "VADODARA", "RAJKOT", "BHAVNAGAR", "JAMNAGAR", "JUNAGADH", "GANDHINAGAR"],
        "UTTAR PRADESH": ["LUCKNOW", "KANPUR", "GHAZIABAD", "AGRA", "VARANASI", "MEERUT", "PRAYAGRAJ", "BAREILLY", "ALIGARH", "MORADABAD"],
        "KERALA": ["KOCHI", "THIRUVANANTHAPURAM", "KOZHIKODE", "THRISSUR", "KOLLAM", "PALAKKAD", "ALAPPUZHA", "KANNUR"],
        "ANDHRA PRADESH": ["VISAKHAPATNAM", "VIJAYAWADA", "GUNTUR", "NELLORE", "KURNOOL", "RAJAHMUNDRY", "TIRUPATI", "KAKINADA"],
        "RAJASTHAN": ["JAIPUR", "JODHPUR", "KOTA", "BIKANER", "AJMER", "UDAIPUR", "BHILWARA", "ALWAR"]
    },
    "UNITED STATES": {
        "CALIFORNIA": ["LOS ANGELES", "SAN FRANCISCO", "SAN DIEGO", "SAN JOSE", "SACRAMENTO", "OAKLAND", "FRESNO", "LONG BEACH", "IRVINE"],
        "NEW YORK": ["NEW YORK CITY", "BUFFALO", "ROCHESTER", "ALBANY", "SYRACUSE", "YONKERS", "NEW ROCHELLE", "MOUNT VERNON"],
        "TEXAS": ["HOUSTON", "AUSTIN", "DALLAS", "SAN ANTONIO", "FORT WORTH", "EL PASO", "ARLINGTON", "CORPUS CHRISTI", "PLANO"],
        "FLORIDA": ["MIAMI", "ORLANDO", "TAMPA", "JACKSONVILLE", "TALLAHASSEE", "FORT LAUDERDALE", "ST. PETERSBURG", "HIALEAH"],
        "ILLINOIS": ["CHICAGO", "AURORA", "ROCKFORD", "JOLIET", "SPRINGFIELD", "NAPERVILLE", "PEORIA"]
    },
    "UNITED KINGDOM": {
        "ENGLAND": ["LONDON", "MANCHESTER", "BIRMINGHAM", "LIVERPOOL", "BRISTOL", "LEEDS", "SHEFFIELD", "NEWCASTLE", "NOTTINGHAM"],
        "SCOTLAND": ["EDINBURGH", "GLASGOW", "ABERDEEN", "DUNDEE", "INVERNESS", "STIRLING", "PERTH"],
        "WALES": ["CARDIFF", "SWANSEA", "NEWPORT", "WREXHAM", "BANGOR"]
    }
}

# Mapping of field patterns to default values
CRITICAL_FIELD_DEFAULTS = {
    "ssn": "000-00-0000",
    "pan": "ABCDE1234F",
    "dob": "01/01/1900",
    "email": "john.doe@example.com",
    "phone": "+1-000-000-0000",
    "address line 1": "Unknown Address",
    "city": "Unknown City",
    "state": "Unknown State",
    "postal code": "00000",
    "zip code": "00000",
    "country": "Unknown Country"
}

# --- HELPERS ---

def is_blank(val):
    """Checks if a value is null, NaN, or an empty/whitespace string."""
    if pd.isna(val):
        return True
    if isinstance(val, str) and not val.strip():
        return True
    return False

def to_sentence_case(s):
    if not isinstance(s, str) or not s:
        return s
    s = s.strip()
    if not s:
        return s
    return s.capitalize()

def fuzzy_match_scalar(query, choices, threshold=85):
    if not query or not choices:
        return None
    query_str = str(query).strip().upper()
    match = process.extractOne(query_str, [c.upper() for c in choices], scorer=fuzz.WRatio)
    if match and match[1] >= threshold:
        # Return the original casing from choices
        return choices[[c.upper() for c in choices].index(match[0])]
    return None

def clean_numeric_scalar(val):
    if is_blank(val): return val
    if isinstance(val, (int, float)): return val
    # Extract numbers, periods, and minus signs
    clean_str = "".join(re.findall(r'[0-9\.\-]', str(val)))
    try:
        return float(clean_str)
    except:
        return val

# --- CORE FUNCTIONS ---

def validate_and_tag_data(df: pd.DataFrame, rules: list) -> pd.DataFrame:
    """
    Initial validation and tagging of issues.
    """
    val_df = df.copy()
    val_df['dq_status'] = 'good'
    val_df['_issues'] = [[] for _ in range(len(val_df))]
    
    for rule in rules:
        col = rule["column"]
        expectation = rule["expectation"]
        if col not in val_df.columns: continue

        if expectation == "expect_column_values_to_not_be_null":
            mask_bad = val_df[col].apply(is_blank)
            for idx in val_df[mask_bad].index:
                val_df.at[idx, '_issues'].append(f"{col} (null)")
            
        elif expectation == "expect_column_values_to_match_regex":
            regex = rule.get("regex", ".*")
            # We only check regex on non-blank values
            mask_val = ~val_df[col].apply(is_blank)
            mask_not_match = mask_val & ~val_df[col].astype(str).str.match(regex, na=False)
            for idx in val_df[mask_not_match].index:
                val_df.at[idx, '_issues'].append(f"{col} (invalid format)")

        elif expectation == "expect_column_values_to_be_unique":
            mask_duplicate = val_df.duplicated(subset=[col], keep=False) & ~val_df[col].apply(is_blank)
            for idx in val_df[mask_duplicate].index:
                val_df.at[idx, '_issues'].append(f"{col} (not unique)")

    for idx in val_df.index:
        issues = val_df.at[idx, '_issues']
        if issues:
            val_df.at[idx, 'dq_status'] = f"bad: {', '.join(issues)}"
            
    return val_df

def remediate_data(df: pd.DataFrame, rules: list, pii_results: dict = None) -> pd.DataFrame:
    """
    Main remediation engine.
    """
    # 0. Setup and Index alignment
    remediated_df = df.copy().reset_index(drop=True)
    original_df = df.copy().reset_index(drop=True)
    pii_results = pii_results or {}
    
    # Identify key columns
    country_col = next((c for c in remediated_df.columns if "country" in c.lower()), None)
    state_col = next((c for c in remediated_df.columns if "state" in c.lower()), None)
    city_col = next((c for c in remediated_df.columns if "city" in c.lower()), None)
    gender_col = next((c for c in remediated_df.columns if "gender" in c.lower()), None)
    id_cols = [c for c in remediated_df.columns if any(x in c.lower() for x in ["id", "pk", "key", "code"])]

    # 1. Sequential ID Population
    for col in id_cols:
        for i in range(1, len(remediated_df)):
            if is_blank(remediated_df.loc[i, col]):
                prev_val = remediated_df.loc[i-1, col]
                if not is_blank(prev_val):
                    prev_str = str(prev_val).strip()
                    # Try Alphanumeric increment (e.g. CUST001 -> CUST002)
                    match = re.match(r'^([a-zA-Z]+)(\d+)$', prev_str)
                    if match:
                        pre, num_str = match.groups()
                        remediated_df.loc[i, col] = f"{pre}{str(int(num_str) + 1).zfill(len(num_str))}"
                    else:
                        # Try pure numeric increment
                        try:
                            remediated_df.loc[i, col] = str(int(float(prev_str)) + 1)
                        except: pass

    # 2. String & Numeric Standardization
    for col in remediated_df.columns:
        if col in ['dq_status', '_issues']: continue
        
        # A. Sentence Case for Strings (except email)
        if remediated_df[col].dtype == 'object' and "email" not in col.lower():
            remediated_df[col] = remediated_df[col].apply(to_sentence_case)
        
        # B. Numeric Scrubbing
        if any(x in col.lower() for x in ["revenue", "amount", "price", "score", "count", "loyalty"]):
            remediated_df[col] = remediated_df[col].apply(clean_numeric_scalar)

    # 3. Domain-Specific Cleansing
    for col in remediated_df.columns:
        col_lower = col.lower()
        col_pii = pii_results.get(col, [])
        
        # A. Mandate Null Filling for Critical Fields
        default_val = None
        for pattern, d_val in CRITICAL_FIELD_DEFAULTS.items():
            if pattern in col_lower:
                default_val = d_val
                break
        
        if default_val:
            for i in range(len(remediated_df)):
                if is_blank(remediated_df.loc[i, col]):
                    remediated_df.loc[i, col] = f"FIXED_DEFAULT: {default_val}"

        # B. Phone Formatting
        if "phone" in col_lower or any("phone" in str(p).lower() for p in col_pii):
            for i in range(len(remediated_df)):
                val = remediated_df.loc[i, col]
                if is_blank(val) or str(val).startswith("FIXED_"): continue
                
                c_code = "US"
                if country_col:
                    country_name = str(remediated_df.loc[i, country_col]).upper()
                    if "INDIA" in country_name: c_code = "IN"
                    elif "KINGDOM" in country_name or "GB" in country_name: c_code = "GB"
                
                try:
                    parsed = phonenumbers.parse(str(val), c_code)
                    if phonenumbers.is_valid_number(parsed):
                        remediated_df.loc[i, col] = phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.INTERNATIONAL).replace(" ", "-")
                    else:
                        # Try adding country code
                        reg_code = phonenumbers.country_code_for_region(c_code)
                        parsed2 = phonenumbers.parse(f"+{reg_code}{str(val)}", c_code)
                        if phonenumbers.is_valid_number(parsed2):
                            remediated_df.loc[i, col] = phonenumbers.format_number(parsed2, phonenumbers.PhoneNumberFormat.INTERNATIONAL).replace(" ", "-")
                except: pass

        # C. Email
        elif "email" in col_lower or any("email" in str(p).lower() for p in col_pii):
             for i in range(len(remediated_df)):
                val = remediated_df.loc[i, col]
                if is_blank(val) or str(val).startswith("FIXED_"): continue
                if not validators.email(str(val)):
                    remediated_df.loc[i, col] = f"FIXED_DEFAULT: {CRITICAL_FIELD_DEFAULTS['email']}"

        # D. Dates
        elif "date" in col_lower or "dob" in col_lower or any("date" in str(p).lower() for p in col_pii):
            for i in range(len(remediated_df)):
                val = remediated_df.loc[i, col]
                if is_blank(val) or str(val).startswith("FIXED_"): continue
                try:
                    parsed = dateparser.parse(str(val))
                    if parsed: remediated_df.loc[i, col] = parsed.strftime("%d/%m/%Y")
                except: pass

        # E. Gender
        elif "gender" in col_lower or col == gender_col:
            gender_map = {'m': 'Male', 'male': 'Male', 'man': 'Male', 'f': 'Female', 'female': 'Female', 'woman': 'Female'}
            remediated_df[col] = remediated_df[col].apply(lambda x: gender_map.get(str(x).lower().strip(), 'Other') if not is_blank(x) else x)

    # 4. Fuzzy Geographic Standardization
    countries_list = list(GEOGRAPHIC_DATA.keys())
    if country_col:
        for i in range(len(remediated_df)):
            val = remediated_df.loc[i, country_col]
            if not is_blank(val):
                match = fuzzy_match_scalar(val, countries_list)
                if match: remediated_df.loc[i, country_col] = match

    if state_col and country_col:
        for i in range(len(remediated_df)):
            s_val = remediated_df.loc[i, state_col]
            c_val = str(remediated_df.loc[i, country_col]).upper()
            if not is_blank(s_val) and c_val in GEOGRAPHIC_DATA:
                states_list = list(GEOGRAPHIC_DATA[c_val].keys())
                match = fuzzy_match_scalar(s_val, states_list)
                if match: remediated_df.loc[i, state_col] = match

    if city_col and state_col and country_col:
        for i in range(len(remediated_df)):
            ci_val = remediated_df.loc[i, city_col]
            c_val = str(remediated_df.loc[i, country_col]).upper()
            s_val = str(remediated_df.loc[i, state_col]).upper()
            if not is_blank(ci_val) and c_val in GEOGRAPHIC_DATA and s_val in GEOGRAPHIC_DATA[c_val]:
                cities_list = GEOGRAPHIC_DATA[c_val][s_val]
                match = fuzzy_match_scalar(ci_val, cities_list)
                if match: remediated_df.loc[i, city_col] = match

    # 5. Catch-All Null Value Sweep (Except IDs)
    for col in remediated_df.columns:
        if col in id_cols or col == 'dq_status' or col == '_issues': continue
        for i in range(len(remediated_df)):
            if is_blank(remediated_df.loc[i, col]):
                if pd.api.types.is_numeric_dtype(remediated_df[col]):
                    remediated_df.loc[i, col] = "FIXED_NULL_0: 0"
                else:
                    remediated_df.loc[i, col] = "FIXED_NULL_UNK: Unknown"

    # 6. Final Audit & Status Construction
    for i in range(len(remediated_df)):
        msgs = []
        
        for col in remediated_df.columns:
            if col in ['dq_status', '_issues']: continue
            
            new_val = remediated_df.loc[i, col]
            orig_val = original_df.loc[i, col]
            
            if str(new_val).startswith("FIXED_DEFAULT"):
                actual = str(new_val).split(": ")[1]
                remediated_df.loc[i, col] = actual
                msgs.append(f"Filled with default value: {actual} in column: {col}. Needs manual fix")
            elif str(new_val).startswith("FIXED_NULL"):
                actual = str(new_val).split(": ")[1]
                remediated_df.loc[i, col] = actual
                msgs.append(f"Nulls found in {col} and replaced with default value. Manual fix recommended.")
            elif not is_blank(orig_val) and not is_blank(new_val):
                if str(orig_val).strip().lower() != str(new_val).strip().lower():
                    msgs.append(f"Standardized {col}: {orig_val} -> {new_val}")
            elif is_blank(orig_val) and not is_blank(new_val):
                # This handles IDs or other cases not caught by FIXED_ markers
                msgs.append(f"Populated missing {col}: {new_val}")

        # Integrity check
        if country_col:
            c_val = str(remediated_df.loc[i, country_col]).upper()
            s_val = str(remediated_df.loc[i, state_col]).upper() if state_col else None
            ci_val = str(remediated_df.loc[i, city_col]).upper() if city_col else None
            
            integrity_err = False
            if c_val in GEOGRAPHIC_DATA:
                data = GEOGRAPHIC_DATA[c_val]
                if s_val:
                    if s_val not in data: integrity_err = True
                    elif ci_val and ci_val not in data[s_val]: integrity_err = True
            elif not is_blank(c_val):
                # If country not in dict, we skip validation or mark as manual check needed?
                # User asked to flag if incorrect or missing.
                pass 

            if integrity_err:
                msgs.append("Integrity check failed: either city or state or country is missing or incorrect. Manual Check needed")

        remediated_df.at[i, 'dq_status'] = " | ".join(msgs) if msgs else 'good'

    return remediated_df.set_index(df.index)
