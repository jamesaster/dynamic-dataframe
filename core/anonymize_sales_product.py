import pandas as pd

# 7. Applying anonymized product infos
def anonymize_sales_product(df: pd.DataFrame, anonym_path: str='../data_output/Anonym_Price.csv') -> pd.DataFrame:
    """
    ## Stage 6 - Applying anonymized product infos
    This function injects anonymized product features into the main sales dataframe.
    It drops sensitive columns and merges the anonymized price dataframe on 'sku'.
    """
    if isinstance(anonym_path, pd.DataFrame):
        anonym_price = anonym_path
    else:
        anonym_price = pd.read_csv(anonym_path)

    # Preparing Main df
    sensitive_cols = ['cat', 'sap_article', 'sap_description']
    df = df.drop(sensitive_cols, axis=1, errors='ignore')

    # Preparing price df
    anonym_price['detail_sub_lob'] = anonym_price['detail_sub_lob'].astype('string').str.strip().str.title()
    anonym_price = anonym_price.rename({'master_sku': 'sku'}, axis=1)
    anonym_cols = ['sku', 'cat', 'product_name', 
        'detail_sub_lob', 'color', 
        'memory_size', 'new_price']
    
    # Merging main DataFrame
    df_ready = df.merge(anonym_price[anonym_cols], how='left', on='sku')

    return df_ready