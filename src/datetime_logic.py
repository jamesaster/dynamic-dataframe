import pandas as pd

# ---- Nhóm Date -----
def chunks_maker(df: pd.DataFrame, ffilled_bfilled_date: str)-> dict:
#* Gom chunk bằng:  missing.indexs.diff().cumsum()
#* df = .zip(index, cumsum).groupby....> to_dict()
    # Indexs of NaT rows -> Index_Obj
    idx_of_nat = df.loc[df[ffilled_bfilled_date].isna(),:].index
    # Convert Index_Obj -> Series (values = Boolean after diff)
    series_error_diff = idx_of_nat.to_series().diff() > 1
    # Tổng tích lũy của Series Boolean (nếu index nối tiếp thì +0, đứt quãng +1)
    list_error_cumsum = series_error_diff.cumsum().to_list()
    # Tạo data_frame zip(index_lỗi, đánh dấu group lỗi)
    df_chunk = pd.DataFrame(zip(idx_of_nat, list_error_cumsum), columns=['idx', 'cumsum'])
    # Tạo dict groupby(cumsum)[idx] để tách các chunk ra
    df_chunk_to_dict = df_chunk.groupby('cumsum')['idx'].apply(list).to_dict()
    # Loại các chunk > 5 rows
    # filtered_chunks = {k: v if len(v) <= 3 else 'over the limit' for k, v in df_chunk_to_dict.items()}
    filtered_chunks = {k:v for k, v in df_chunk_to_dict.items() if len(v) <= 5}
    return filtered_chunks
def validate_n_correct_chunks(df: pd.DataFrame, dict_chunks: dict, raw_date: str, fill_date: str)-> list:
    pending_date_idx = []
    for _ , a_chunk in dict_chunks.items():
        p, n = min(a_chunk) - 1, max(a_chunk) + 1
        if p < 0 or n not in df.index:
            print(f"Skipping Chunk {a_chunk} = NA")
            continue
    #*------------------------
    # If prev or next is empty
        prev_pd, next_pd = df.at[p,fill_date], df.at[n,fill_date]
        if pd.isna(prev_pd) or pd.isna(next_pd):
            print(f'    prev or next is empty for chunk {a_chunk}')
            continue
    #*------------------------
    # Case 1: prev_pd == next_pd
        if prev_pd == next_pd:
            df.loc[a_chunk, fill_date] = prev_pd
            print(f'    [Match Both_day] Index {a_chunk} assigned {prev_pd.date()}')
            continue
    #*------------------------
    # Case 2.0: Stop if max gap < 4
        the_gap = (next_pd - prev_pd).days
        if the_gap >= 4:
            print(f'Chunk {a_chunk} has gap too large.')
            continue
    # Case 2.1: next_pd - prev_pd <= 3  and > 0
        if the_gap <= 3 and the_gap > 0:
    #*      Gom Set trước khi loop idx
            set_prev = {prev_pd.day, prev_pd.month, prev_pd.year}
            set_next = {next_pd.day, next_pd.month, next_pd.year}
            for idx in a_chunk:
                idx_raw = df.at[idx, raw_date]
                if pd.isna(idx_raw) or str(idx_raw).strip() == '':
                    pending_date_idx.append(idx)
                    print(f'    NGHI VẤN: Index {idx} nội dung là: {df.at[idx, raw_date]}')
                    continue
    #*      Similarity Check - (Set Validation)
                try:
                    idx_split =  idx_raw.replace(' ','/').replace('-','/').strip().split('/')
                    set_current = {int(x) for x in idx_split if x.strip().isdigit()}
                except(ValueError, TypeError, AttributeError):
                    pending_date_idx.append(idx)
                    continue

                not_prev, not_next = list(set_current - set_prev), list(set_current - set_next)

            # Nếu sau khi trừ set mà cả 2 dư ra 2 thành phần: continue
                if len(not_prev) >= 2 and len(not_next) >= 2:
                    pending_date_idx.append(idx)
                    print(f'    Something wrong at index: {idx} fill_date')
                    continue
            # If match Prev_date:
                if not not_prev:
                    df.at[idx, fill_date] = prev_pd
                    print(f'    [Match Prev_day] Index {idx} assigned {prev_pd.date()}')
            # If match Next_date:
                elif not not_next:
                    df.at[idx, fill_date] = next_pd
                    print(f'    [Match Next_day] Index {idx} assigned {next_pd.date()}')
                # Thay vì tự ghép, hãy đánh dấu để kiểm tra bằng tay
                else:
                    pending_date_idx.append(idx)
                    print(f'    NGHI VẤN: Index {idx} nội dung là: {df.at[idx, raw_date]}')
    return pending_date_idx

#!  recover_date: main
def recover_date(df: pd.DataFrame, date_raw: str, anchor_col_name: str=None)-> list:
    print(f'df.index: {df.index}')
    if not df.index.is_monotonic_increasing:
        df = df.sort_index()
        print("Warning: Index was not sorted. DataFrame has been sorted automatically.")

    list_if_error = []

    if date_raw:
        d1 = pd.to_datetime(df[date_raw], format='%Y-%m-%d', errors='coerce')
        d2 = pd.to_datetime(df[date_raw], format='%d-%m-%Y', errors='coerce')
        df['fill_date'] = d1.fillna(d2)

#* Nếu có Anchor_col > ffill, bfill trước.
    if anchor_col_name:
        df['fill_date'] = df.groupby(anchor_col_name)['fill_date'].transform('ffill')
        df['fill_date'] = df.groupby(anchor_col_name)['fill_date'].transform('bfill')
        print(f'    Missing date detected, proceed ffill & bffll by anchor "{anchor_col_name}"')

    number_of_errors_date = df['fill_date'].isna().sum()
    if number_of_errors_date > 0:   
        the_chunks = chunks_maker(df, 'fill_date')
        if the_chunks:
            list_if_error = validate_n_correct_chunks(df, the_chunks, date_raw, 'fill_date')
    if not list_if_error:
        print(f"    Number of NaT in 'fill_date': {df['fill_date'].isna().sum()}")
    return df['fill_date'], list_if_error

# ---- Nhóm Time -----
def time_format(df, time_col_name)-> pd.Series:      
    if time_col_name:
        t1 = pd.to_datetime(df[time_col_name], format='%I:%M%p' , errors='coerce')
        t2 = pd.to_datetime(df[time_col_name], format='%H:%M:%S', errors='coerce') 
        t3 = pd.to_datetime(df[time_col_name], format='%H:%M'   , errors='coerce') 

    return t1.fillna(t2).fillna(t3)
