"""
Process Product Master Data
config (prod_salt, ran_seed, scale_dict) được truyền vào từ ngoài.
"""

import pandas as pd
import numpy as np
import base64

def extract_color(row, color_keywords):
    """Trích xuất màu sắc từ product_name và sap_description"""
    text = str(row.get('product_name', '')) + " " + str(row.get('sap_description', ''))
    text = text.lower().replace('/', ' ').replace('-', ' ')

    for color in sorted(color_keywords, key=len, reverse=True):
        if color in text:
            mapping = {
                'blk': 'Black', 'black': 'Black',
                'gry': 'Gray', 'gray': 'Gray', 'grey': 'Gray',
                'pnk': 'Pink',
                'wht': 'White',
                'blu': 'Blue',
                'grn': 'Green',
                'org': 'Orange',
                'ylw': 'Yellow',
                'crm': 'Cream'
            }
            return mapping.get(color, color.title().replace('gray', 'Gray').replace('grey', 'Gray'))
    return None

def price_scale(df: pd.DataFrame, cat_col: str, price_col: str, scale_dict: dict, ran_seed: int) -> pd.DataFrame:
    """Anonymize giá theo scale_dict với noise"""
    np.random.seed(ran_seed)

    for key, inner_dict in scale_dict.items():
        rate_min = inner_dict['scale'][0]
        rate_max = inner_dict['scale'][1]
        mag = inner_dict['mag']
        sfx = inner_dict['sfx']

        mask = df[cat_col] == key
        if not mask.any():
            continue

        raw_coord = df.loc[mask, price_col]

        price_min = raw_coord.nsmallest(2).iloc[-1]
        price_max = raw_coord.nlargest(1).iloc[0]

        noise = pd.Series(
            np.random.normal(1.0, 0.0112, size=len(raw_coord)),
            index=raw_coord.index
        )

        interp_factor = np.interp(
            raw_coord,
            [price_min, price_max],
            [rate_min, rate_max]
        )

        new_price = raw_coord * interp_factor * noise
        df.loc[mask, 'new_price'] = np.floor(new_price / mag) * mag + sfx

    return df

def process_product_master(
    product_info: str,
    prod_salt: int,
    ran_seed: int,
    scale_dict: dict,
    output_path: str = '../CSV_write/Anonym_Price.csv'
) -> pd.DataFrame:
    """
    Hàm chính xử lý product master
    
    Parameters:
        product_info (str): Đường dẫn đến file CSV input
        prod_salt (int): Salt dùng để tạo master_sku
        ran_seed (int): Random seed cho price scaling
        scale_dict (dict): Dictionary scale giá từ DYNAMIC_PRICE.json
        output_path (str): Đường dẫn file output
    
    Returns:
        pd.DataFrame: DataFrame đã xử lý
    """

    print("Đang đọc file sản phẩm...")
    product_master = pd.read_csv(product_info)

    # CHUẨN HÓA CỘT 
    product_master.columns = (
        product_master.columns
        .astype('string')
        .str.strip()
        .str.replace(' ', '_')
        .str.lower()
    )

    # Drop blank EAN và duplicates
    product_master = product_master.dropna(subset='ean') \
                                  .drop_duplicates(subset='ean', ignore_index=True)

    # EAN to numeric
    product_master['ean'] = pd.to_numeric(product_master['ean'], errors='coerce')

    valid_ean = product_master['ean'].notna().mean() * 100
    if valid_ean < 100:
        print(f'⚠️  EAN valid = {valid_ean:.2f}%')

    # TẠO MASTER SKU
    product_master.insert(
        1, 'master_sku',
        product_master['ean'].apply(
            lambda x: base64.b32encode(
                int(x + prod_salt).to_bytes(7, 'big')
            ).decode().rstrip('=') if not pd.isna(x) else None
        )
    )

    is_apple = product_master['cat'].isin([
        'ACCESSORIES (APPLE)', 'IPAD', 'IPHONE', 'MAC', 'WATCH'
    ])

    app_ean = 'APP-' + product_master['master_sku'].str[2:7] + '-' + product_master['master_sku'].str[7:]
    third_ean = '3RD-' + product_master['master_sku'].str[2:7] + '-' + product_master['master_sku'].str[7:]

    product_master['master_sku'] = np.where(is_apple, app_ean, third_ean)

    # COLOR & MEMORY SIZE 
    print("Đang trích xuất Color và Memory Size...")

    color_keywords = [
        "black titanium", "white titanium", "desert titanium", "natural titanium",
        "black", "white", "silver", "gold", "space grey", "space gray", "midnight",
        "starlight", "pink", "blue", "green", "purple", "red", "yellow", "orange",
        "teal", "ultramarine", "clay", "guava", "cypress", "winter blue", "storm blue",
        "elderberry", "slate blue", "abyss blue", "dark cherry", "forest green", "ink",
        "umber", "lilac", "succulent", "sunglow", "olive", "soft mint", "light blue",
        "sunshine", "taupe", "mulberry", "pacific blue", "evergreen", "indigo",
        "pride edition", "bright orange", "clover", "moss green", "golden brown",
        "sequoia green", "wisteria", "marigold", "pink pomelo", "blue jay",
        "lemon zest", "eucalyptus", "nectarine", "blue fog", "english lavender",
        "marine blue", "canary yellow", "sky", "gray", "grey", "clear", "transparent",
        "blk", "gry", "pnk", "wht", "blu", "grn", "org", "ylw", "crm"
    ]

    product_master['color'] = product_master.apply(
        lambda row: extract_color(row, color_keywords), axis=1
    )

    # Extract Memory Size
    regex = r'(?i)(\d{1,4})\s*(GB|TB|mm)'
    extract = product_master['product_name'].str.extract(regex, expand=False)
    product_master['memory_size'] = extract[0].fillna('') + extract[1].fillna('')

    product_master.loc[
        product_master['cat'].str.contains('3RD ACC|DEMO', case=False, na=False),
        'memory_size'
    ] = None

    product_master['memory_size'] = product_master['memory_size'].replace(['', 'nan'], None)
    product_master['memory_size'] = product_master['memory_size'].str.replace(
        r'(\d+)([A-Za-z]+)', r'\1\2', regex=True
    )

    # TẠO NEW PRODUCT NAME 
    print("Đang tạo tên sản phẩm chuẩn hóa...")

    is_color = product_master['color'].notnull()
    is_memory = product_master['memory_size'].notnull()
    is_apple_device = is_apple & is_color
    is_sub_cat = product_master['detail_sub_lob'].notnull()

    # Apple devices
    product_master['new_product_name'] = np.where(
        is_apple_device,
        product_master['detail_sub_lob'].fillna('') + ' ' +
        product_master['color'] + ' ' +
        np.where(is_memory, product_master['memory_size'], ''),
        None
    )

    # Non-Apple
    non_apple = ~is_apple_device & is_color & is_sub_cat
    product_master['new_product_name'] = np.where(
        non_apple,
        product_master['detail_sub_lob'] + ' ' + product_master['color'],
        product_master['new_product_name']
    )

    other = product_master['new_product_name'].isnull()
    product_master.loc[other, 'new_product_name'] = product_master['product_name']

    product_master['new_product_name'] = product_master['new_product_name'].str.strip().str.upper()
    product_master['product_name'] = product_master['new_product_name']

    #  ANONYMIZE GIÁ 
    print("Đang anonymize giá...")
    product_master = price_scale(product_master, 'cat', 'price', scale_dict, ran_seed)

    #  CLEANUP
    final_drop = ['sap_article', 'sap_description', 'new_product_name', 'price']
    product_master = product_master.drop(columns=final_drop, errors='ignore')

    # SAVE 
    product_master.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f'Saved product_master: {output_path}')
    print(f'Tổng số dòng: {len(product_master):,}')

    return product_master

