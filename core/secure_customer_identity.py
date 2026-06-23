import pandas as pd
import numpy as np
from src import cus_normalize, create_cust_master, base32_encode, create_cus_id

def anonymize_customer_pii(df:pd.DataFrame, config:dict, cust_salt:str) -> pd.DataFrame:
    """
    ## Stage 3 - Identity Masking & Privacy Protection
    Anonymize customer PII data (phone, name, email) via base32 encoding with salt.
    Steps:
    1. Normalize phone, name, email (cus_normalize)
    2. Create Cust Master with unique phone and corresponding name/email (create_cust_master)
    3. Base32 encode the Cust Master phone column with salt (base32_encode)
    4. Create a unique customer ID in Cust Master by combining encoded phone with name/email (create_cus_id)
    5. Map the encoded phone and customer ID back to the main dataframe for downstream use.
    
    Note: This approach allows us to maintain a consistent pseudonymous identifier for each customer across datasets without exposing actual PII, while also enabling potential re-identification if necessary by retaining the mapping in the Cust Master.
    
    Args:
        df: Input dataframe containing customer data.
        config: Dictionary containing configuration options, including 'anonymous' keys for phone, name, and email columns.
        cust_salt: A string salt used for encoding to enhance security.
    
    Returns:
        df: Dataframe with anonymized customer identifiers and mapped customer ID.
    """

    # NOTE: Encrypting customer PII (Phone/Name/Email) using Salted Base32.

    cust_cols = config.get('anonymous')
    _p, _n, _e = cust_cols

    # Normalize before create Cust Master
    df = cus_normalize(df, _p, _n, _e)

    # Creating Cust Master
    df_cust_master = create_cust_master(df, _p, _n, _e)
    df_cust_master = base32_encode(df_cust_master, _p, cust_salt)
    df_cust_master = create_cus_id(df_cust_master, _p, _n, _e)

    # Encode main df[phone] (for mapping)
    df = base32_encode(df, _p, cust_salt).drop([ _n, _e], axis=1, errors='ignore')

    # Map Cust Master back to main df
    df = df.merge(df_cust_master, how='left', on=_p)
    
    return df
