import pandas as pd
import base64
import re

def cus_normalize(df: pd.DataFrame, _p: str, _n: str, _e: str)-> pd.DataFrame:
    customer = [_p, _n, _e]
   
    #! 1. lower  >   replace(r'\s+')  >   strip
    df[customer] = df[customer].apply(lambda col: col.astype('string').str.lower().str.replace(r'\s+', ' ',regex=True).str.strip())

    #! 2. Filter email
    e_mask = df[_e].str.contains(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', regex=True)
    df[_e] = df[_e].where(e_mask, None)

    #! 3. phone.str.replace('\D'=not_a_number, '^84'=start_with: 84) 
    df[_p] = df[_p].str.replace(r'\D','', regex=True).str.replace(r'^84','0', regex=True)
    # Phone Masks
    p_len_equal_9      = df[_p].str.len() == 9
    p_len_less_9       = df[_p].str.len() < 9
    p_start_0          = df[_p].str.contains(r'^0', regex=True)
    p_start0_equal9    = p_len_equal_9 & p_start_0
    p_notstart0_equal9 = p_len_equal_9 & ~p_start_0
    p_lenmorethan_12      = df[_p].str.len() >= 12
    # Phone fixing
    df.loc[p_len_less_9, _p]       =      None                              # <9
    df.loc[p_start0_equal9, _p]    =      None                              # ^0 & ==9
    df.loc[p_notstart0_equal9, _p] = '0' + df.loc[p_notstart0_equal9, _p]   # ~^0 & ==9
    df.loc[p_lenmorethan_12, _p]   =      None

    #! Name.title()
    df[_n] = df[_n].replace('unknown', None).str.title()

    return df
def create_cust_master(df: pd.DataFrame, phone: str, name: str, email: str)-> pd.DataFrame:
    phone_name_email = [phone, name, email]
    return (df[phone_name_email].assign(
        name = lambda x: x[name].astype(str).str.title(),
        _p = lambda x: x[phone].astype(str).str.len(),
        _n = lambda x: x[name].astype(str).str.len(),
        _e = lambda x: x[email].astype(str).str.len() 
        ).sort_values(by=['_p','_n','_e'],
        axis= 0,
        ignore_index=True,
        ascending=[True,False,False]).groupby(phone, as_index=False)            
        .head(1)    
        .drop(['_p','_n','_e'], axis=1)
        .sort_values(by=[phone, name],
        ascending=[True, True])
        .reset_index(drop=True))            
    # Nếu groupby không có as_index=False thì nó sẽ cho phone=index
    # Nếu không có lệnh SELECT hoặc AGG() sau khi groupby, kết quả sẽ ở trạng thái super position (object quần què không hiển thị được ở dạng df)
    # Nếu sau groupby() có select [[col1,col2,.]] thì sẽ là 1 bản sao mới không tồn tại ['_p','_n','_e'] (không drop đc)
    # Groupby.head làm loạn index nên cuối cùng vẫn phải reset_index 
def base32_encode(df: pd.DataFrame, _p: str, cust_salt:int)-> pd.DataFrame:

    # 1. Encode phone
    df[_p] = pd.to_numeric(df[_p], errors='coerce').astype('Int64')
    df[_p] = (df[_p]
            .apply(lambda x: 
            base64.b32encode(
            int(x + cust_salt).to_bytes(5, 'big'))
            .decode().rstrip('=') if pd.notna(x) else None ))

    return df
def create_cus_id(df_cust_master: pd.DataFrame, _p: str, _n: str, _e: str)-> pd.DataFrame:
    df_cust_master.insert(0, 'id', 
    'CUS-' + (df_cust_master[_p].str[ :4] + '-' 
            + df_cust_master[_p].str[4: ])
            .str.upper())
    print(f'Created "id" column for df_cust_master')

    # Mask <Asterisk>@email
    df_cust_master[_e] = df_cust_master[_e].apply(lambda x: re.sub(r'(?<=^.)(.+)(?=.@)', lambda m: '*' * len(m.group(1)), str(x)))

    # Clean '<na>string'
    df_cust_master[_e] = df_cust_master[_e].str.replace('<na>', '', case=False)
    df_cust_master[_n] = df_cust_master[_n].str.replace('<na>', '', case=False).str.title()
    return df_cust_master