import pandas as pd

def is_boolean(sample: pd.Series)-> bool:
    if sample.empty: return False
    if sample.dtype == 'bool': return True
    s = sample.astype('string').str.strip().str.lower()
    x = s.isin(['true','false','yes','no','1','0']).mean()
    y = s.nunique() 
    return (x >= 0.9) and (y <= 2)

def is_datetime(sample: pd.Series)-> bool:
    if sample.empty: return False
    if sample.dtype.kind in ['b','i','u','f']: return False
    if sample.dtype.kind == 'M': return True
    x = pd.to_datetime(sample, format='mixed', errors='coerce').notna().mean()
    y = sample.str.contains(r'[-/: ]', na=False).mean()
    return (x >= 0.9) and (y >= 0.9)

def is_alo(sample: pd.Series)-> bool:
    if sample.empty: return False
    s = sample.dropna().astype('string')
    x = s.str[0].isin(['0','3','5','7','8','9']).mean()
    y = s.str.replace(r'[,\-\s]', '', regex=True).str.len().mean()
    return (x >= 0.9) and (9 <= y <= 11)

def is_money(sample: pd.Series)-> bool:
    if sample.empty: return False
    if not sample.dtype.kind == 'o': return False
    pattern = r'^\s*-?\d{1,3}(?:,\d{3})*(?:\.\d+)?\s*$'
    return sample.astype('string').str.strip().str.match(pattern, na=False).mean() >= 0.9

def is_numeric(sample: pd.Series)-> bool:
    if sample.empty: return False
    return pd.to_numeric(sample, errors='coerce').notna().mean() >= 0.9

def is_category(sample: pd.Series)-> bool:
    if sample.empty: return False
    numeric_ratio = pd.to_numeric(sample, errors='coerce').notna().mean()
    nunique = sample.nunique()

    if numeric_ratio > 0.8:
        return False
    return 2 <= nunique <= 33
