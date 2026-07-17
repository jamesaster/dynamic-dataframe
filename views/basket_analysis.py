from mlxtend.frequent_patterns import apriori, association_rules
from visuals.visuals_helper import custom_sort
from typing import Literal
from src.columns import colName as c
from visuals import styled_header
import streamlit as st
import pandas as pd
SS = st.session_state
st.title('🏷️ Basket Analysis')
st.markdown("""
    <div style="font-size: 0.85rem; color: #6880AA; line-height: 1.5; margin-bottom: 35px; margin-top: 0px;">
    <span style="font-size: 1.15rem; font-weight: 800; color: #6880AA; vertical-align: -1px; margin-right: 4px;">Apriori</span> 
    is a data mining algorithm used for <strong>Cross-Selling Optimization</strong>.
    <br>
    This tool uncovers hidden <strong>relationships between items</strong> to identify which items are frequently <strong>bought together</strong>.
    <br>
    <div style="font-size: 0.75rem; font-style: normal; color: #8AA0C4; margin-top: 5px;">
    * Data in this project is simulated based on actual operational trends.
    </div>
    </div> """, unsafe_allow_html=True)

class BasketAnalyzer:
    class Col:
        A       = 'antecedents'
        B       = 'consequents'
        A_sup   = 'antecedent support'
        B_sup   = 'consequent support'
        conf    = 'confidence'
        support = 'support'
        lift    = 'lift'

        pair_id = 'pair_id'
        pattern = 'consequents pattern'
        acc_combo = 'accessories pattern'

    def __init__(self, rules: pd.DataFrame):
        self.rules = rules
    def res(self):
        return self.rules
    def agg_set_pattern(self):
        """ ## Tạo 1 set(rỗng) và union với các pattern_set trong nhóm 'Antecedents' """
        self.rules = (
            self.rules.groupby(self.Col.A)[self.Col.pattern]
            .agg(lambda s: set().union(*s))
            .reset_index()
        )
        return self
    def shorten_apple_name(self):
        allowed_pattern = r'\b(IPHONE(?:\s\d+)?|IPAD(?:\s\d+)?|AIR(?:\s\d+)?|PRO(?:\s\d+)?|MAX(?:\s\d+)?|M\d+|\d+)\b'
        series = self.rules[self.Col.A].astype(str).str.upper()
        self.rules[self.Col.A] = (
            series
            .str.findall(allowed_pattern)
            .str.join(' ')
            .str.replace(r'\s+', ' ', regex=True)
        )
        return self
b = BasketAnalyzer.Col
@st.cache_data
def apriori_defined_rules(sales: pd.DataFrame, device_set: frozenset, min_support: float=0.0005):
    #region #! Matrix / Apriori
    basket_matrix     = pd.crosstab(index=sales[c.invoice], columns=sales[c.sku]).astype(bool)
    frequent_itemsets = apriori(basket_matrix, min_support=min_support, use_colnames=True, low_memory=True)
    keep_columns      = [
        b.A,
        b.B,
        b.A_sup,
        b.B_sup,
        b.conf,
        b.support,
        b.lift
    ]
    rules_raw = association_rules(frequent_itemsets, metric=b.lift, min_threshold=2)[keep_columns]
    #endregion

    rules = rules_raw[rules_raw[b.A].apply(len) == 1].copy()
    
    #region #? rule_map phục vụ nhặt chỉ số có tính chất cặp lẻ A-B (15-07-26)
    rule_single = rules[rules[b.B].apply(len) == 1].copy()
    rule_single[b.A] = [''.join(x) for x in rule_single[b.A]]
    rule_single[b.B] = [''.join(x) for x in rule_single[b.B]]
    get_i      = lambda x: rule_single.columns.get_loc(x)
    rules_keys = rule_single[[b.A, b.B]].to_numpy(str).tolist()
    df_vals    = rule_single.iloc[:, (get_i(b.B) + 2) :] #* bỏ cột A_support
    df_vals.iloc[:, :-1] = df_vals.iloc[:, :-1] * 100    #* 3 chỉ số tính theo % trừ lift
    val_cols   = df_vals.columns
    rules_vals = df_vals.to_numpy(float).tolist()
    rules_map  = {tuple(k): v for k, v in zip(rules_keys, rules_vals)}
    #endregion #?

    # có thể zip 2 series và bốc từng cặp dòng ra để union
    # lưu ý gọi hàm a.union(b) chậm hơn là dùng toán tử a | b
        # Khi làm việc với các object kiểu tập hợp (list, set,..) hoặc chuỗi
        # dùng list comprehension sẽ nhanh hơn apply or map vì không tốn công gọi hàm func/lambda cho mỗi dòng
    rules.loc[:, b.pair_id] = [a | b for a , b in zip(rules[b.A], rules[b.B])]
    rules = rules.sort_values(by=[b.pair_id, b.lift], ascending=[True, False])
    rules = rules.drop_duplicates(subset=b.pair_id, keep='first', ignore_index=True)

    # Loại những dòng vế B có chứa device 
    make_sense = pd.Series([not (set_B & device_set) for set_B in rules[b.B]], index=rules.index)
    rules = rules[make_sense].drop(columns=b.pair_id)

    # Unset A và chuyển B về set thuần
    rules[b.A] = rules[b.A].str.join('')
    list_of_frozenset = rules[b.B].tolist()
    rules[b.pattern]  = pd.Series([set(f_set) for f_set in list_of_frozenset], index=rules.index)

    # Dùng .tolist() để ép series về list chứa các set, pd.df sẽ tự unpack set thành nhiều cột
    splitted_B = pd.DataFrame(list_of_frozenset, index=rules.index)
    splitted_B.columns = b.B + '_' + splitted_B.columns.astype(str)

    rules = pd.concat([splitted_B, rules.drop(columns=b.B)], axis=1).fillna('-')
    columns = [b.A, b.pattern] + rules.columns.drop([b.A, b.pattern]).tolist()
    return rules[columns], rules_map, val_cols
def product_shorten(series: pd.Series, pattern: Literal['device', 'acc'] = 'device'):
    if pattern == 'device':
        series = series.replace({
            r'\bMBA\b': 'MACBOOK AIR',
            r'\bMBP\b': 'MACBOOK PRO',
            r'\bMB\b' : 'MACBOOK'
        }, regex=True)

    allowed_pattern = {
        'device': r'\b(IPHONE|IPAD|WATCH|MAC|MACBOOK|MINI|SE|AIR|PRO|MAX|ULTRA|M\d+|(?<!\.)\d+)\b',
        'acc'   : r'\b(\
            |INNO POWERMAG|AIRPODS(?:\s\d+)?|IPHONE(?:\s\d+)?|LIGHTNING|CROSSBODY|KEYBOARD|\
            |CƯỜNG LỰC|ADAPTER|CHARGER|CABLE|CHARGE|PENCIL|APPLE|WATCH|IPAD(?:\s\d+)?|SCREEN|CAMERA|PRIVACY|\
            |\d+MAH|FRONT|UNIQ|ANTI(?:\S*)?|CLEAR|LEN(?:S)?|ULTRA|POWER|HDMI|CÁP|MAX|STRAP|CASE|\
            |GEN(?:\s\d+)?|PRO(?:\s\d+)?|IP(?:\s?\d+)?|AIR(?:\s\d+)?|USB(?:-[C\d]+)?|\d.\d|TO|/|C)\b'
    }[pattern]

    return (
        series
        .str.upper()
        .str.findall(allowed_pattern)
        .apply(lambda x: list(dict.fromkeys(x)))
        .str.join(' ')
        .str.strip() # Vừa strip vừa replace mới ok
        .str.replace(r'\s+', ' ', regex=True)
    )
def list_sku_to_product(sku_list: list):
    return [product_dict.get(sku, '-') for sku in sku_list]

#region #? Setup / Source

months_back = st.sidebar.selectbox('**Look Back**', range(1, 13), index=11, format_func=lambda x: f"Last {x} month{'s' if x > 1 else ''}")
end_date    = pd.to_datetime('30-06-2026', dayfirst=True)
start_date  = end_date - pd.DateOffset(months=months_back)
date_mask   = lambda df: df[c.date].between(start_date, end_date)
sku_mask    = lambda df: df[c.sku] != '-'
qty_mask    = lambda df: df[c.qty]  >  0
requires    = [c.invoice, c.cat, c.sku, c.prod_name, c.qty]
full_sales: pd.DataFrame = SS.get('analysis_sales', None)
full_stock: pd.DataFrame = SS.get('analysis_stock', None)
if full_sales is None or full_stock is None:
    st.info('Switch to dashboard then switch back.')
    st.stop()
sales = full_sales.loc[
    date_mask(full_sales)
    & sku_mask(full_sales)
    & qty_mask(full_sales),
    requires
    ]
no_invoice    = sales[c.invoice].nunique()
target_match  = st.sidebar.slider('**Appearance Frequency**', 2, 10, min(4, months_back), 1)
factor        = target_match / no_invoice

device_mask   = lambda df: ~ df[c.cat].isin(['3RD ACC', 'APPLE ACC'])
device_set    = frozenset(sales.loc[device_mask, c.sku].tolist())
product_map   = sales[[c.sku, c.prod_name]].drop_duplicates(subset=c.sku).set_index(c.sku)[c.prod_name]
product_dict  = product_map.to_dict()
#endregion

#region #? Rules (Third try)
rules, rules_map, val_cols = apriori_defined_rules(sales, device_set, factor)
device_rules = rules.loc[rules[b.A].isin(device_set), [b.A, b.pattern]]
if len(rules) < 3:
    st.info('Please Lower the Appearance Frequency')
    st.stop()

device_map = device_rules[[b.A, b.pattern]].explode(b.pattern, ignore_index=True)
device_map[val_cols] = [rules_map[k] if k in rules_map else [None] * len(val_cols) for k in zip(device_map[b.A], device_map[b.pattern])]
device_map[b.A] = device_map[b.A].map(product_dict).pipe(product_shorten, pattern='device')
device_map = device_map.groupby([b.A, b.pattern], as_index=False).agg({c: 'max' for c in val_cols})
device_map = {i: sub.drop(b.A, axis=1).set_index(b.pattern).to_dict(orient='index') for i, sub in device_map.groupby(b.A)}

device_rules[b.A] = device_rules[b.A].map(product_map).pipe(product_shorten, pattern='device')
group_device = sorted(
    [{b.A: Atd, b.pattern: set.union(*Csq)} 
    for Atd, Csq in device_rules.groupby(b.A)[b.pattern]]
    ,
    key = lambda x: custom_sort(x[b.A])
)

acc_rules = rules.loc[~rules[b.A].isin(device_set), [b.A, b.pattern]]
acc_rules[b.A] = acc_rules[b.A].map(product_map).pipe(product_shorten, pattern='acc')
acc_mask  = ~ acc_rules[b.A].str.contains(r'^\s*$|^\s*/|(?:\s*cable)', case=False, regex=True)
acc_rules = acc_rules[acc_mask]
group_acc = [{b.A: head, b.pattern: g_tail} for head, tail in acc_rules.groupby(b.A)[b.pattern] if len(g_tail := set.union(*tail)) >= 3]

if not (group_device and group_acc):
    st.info('Please Lower the Appearance Frequency')
    st.stop()

summary, result = st.columns([1, 4], gap='large')
st.html("""
    <style>
    .st-key-device_attachments, .st-key-accessory_bundles {
        background-color: #E8F2FF !important;
        border: 1px solid #FFFFFF !important;
        padding: 2rem;
    }
    div[data-testid="stButton"] button p {
        font-family: monospace !important;
    }
    </style>
""")
@st.fragment
def basket_card(
    df_show     : pd.DataFrame,
    col_config  : dict,
    antecedent  : str,
    str_gap     : str,
    icon        : str,
    cons_sku    : list,
    idx         : int
    ):
    if st.button(
        label   = f'{antecedent}{str_gap}| {len(cons_sku):02d}',
        width   = 'stretch',
        icon    = icon,
        key     = f'{antecedent}_basket_card_{idx}'
    ):
        @st.dialog(antecedent, width='large', icon='📦')
        def show_dialog_df(df: pd.DataFrame, column_config: dict):
            colmap      = dict(zip([x['label'] for x in column_config.values()], list(column_config)))
            options     = list(colmap)
            container   = st.container(border=False, horizontal_alignment='center')
            sort_by     = container.segmented_control('Sort by (Descending)', options=options, default=options[0], key='Sort_name_freq')
            if sort_by and sort_by != options[0] and (by := colmap[sort_by]) in df.columns:
                df = df.sort_values(by=by, ascending=False)
            st.dataframe(df, column_config=column_config, hide_index=True, height=700)
        show_dialog_df(df_show, col_config)
col_config = {
    c.sku       : st.column_config.TextColumn(c.sku.upper(), width=150),
    b.B_sup     : st.column_config.NumberColumn('Base Frequency', format='%.1f %%', width='small', alignment='center'),
    b.conf      : st.column_config.NumberColumn('Top Attach Rate', format='%.1f %%', width='small', alignment='center'),
    b.support   : st.column_config.ProgressColumn('Top Combo Freq', format='%.1f %%', width='small'),
    b.lift      : st.column_config.NumberColumn('Max Lift', format='%.1f', width='small', alignment='center'),
    }
with summary:
    styled_header('Data Summary')
    st.info(
        f"""
        - Number of Invoice: **{sales[c.invoice].nunique()}**
        - Number of Product: **{sales[c.sku].nunique()}**
        - Number of Device: **{len(device_set)}**
        - Unique antecedents (Product): **{rules[b.A].nunique()}**
        - Unique antecedents (Device): **{rules.loc[rules[b.A].isin(device_set), b.A].nunique()}**
        """
        )
    styled_header('How it works')
    st.info("""
        *Each card represents a **product line**, displaying its frequently **bought together** products.*\n
        The number shows its total associated items.
        **Click** on any **card** to unlock deeper metrics.
        """, icon='💡')
    is_icon = st.segmented_control('**Device Icon**', options=['On', 'Off'], default='Off', width='stretch')
with result:
    icon_pack = {
        'IPHONE'    : ':material/mobile_2:',
        'IPAD'      : ':material/tablet:', 
        'MACBOOK'   : ':material/laptop_mac:',
        'WATCH'     : ':material/fitness_tracker:'
    }
    for container_key, group in {'device_attachments': group_device, 'accessory_bundles': group_acc}.items():
        styled_header(container_key.replace('_', ' ').title())
        with st.container(border=True, key=container_key):
            max_str = min(17, max(len(d[b.A]) for d in group))
            device_suggestions = st.columns(4, gap='large')
            total_cards = len(group)
            rows = (total_cards + 3) // 4
            for idx, subgroup in enumerate(group):
                col_idx     = min(idx // rows, 3)
                antecedent  = subgroup[b.A]
                cons_sku    = sorted(subgroup[b.pattern])
                vals_map    = device_map.get(antecedent, {})
                df_string   = pd.DataFrame({'SKU': cons_sku, 'Product Name': list_sku_to_product(cons_sku)}, index=range(1, len(cons_sku) + 1))
                df_vals     = pd.DataFrame([vals_map.get(sku, {}) for sku in cons_sku], index=df_string.index)
                df_show     = pd.concat([df_string, df_vals], axis=1)
                str_gap     = (max_str - len(antecedent) + 1) * '\u2000'
                icon        = (next((icon_pack[k] for k in icon_pack 
                                if k in antecedent.upper()), None) 
                                    if is_icon == 'On'
                                    and container_key == 'device_attachments' else None)
                with device_suggestions[col_idx]:
                    basket_card(df_show, col_config, antecedent, str_gap, icon, cons_sku, idx)

#endregion