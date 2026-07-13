from mlxtend.frequent_patterns import apriori, association_rules
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
def apriori_defined_rules(sales: pd.DataFrame, device_set: frozenset):
    #region #! Matrix / Apriori
    basket_matrix     = pd.crosstab(index=sales[c.invoice], columns=sales[c.sku]).astype(bool)
    frequent_itemsets = apriori(basket_matrix, min_support=0.0005, use_colnames=True, low_memory=True)
    keep_columns      = [
        b.A,
        b.B,
        b.A_sup,
        b.B_sup,
        b.conf,
        b.support,
        b.lift
    ]
    rules_raw = association_rules(frequent_itemsets, metric=b.lift, min_threshold=3)[keep_columns]
    #endregion

    rules = rules_raw[rules_raw[b.A].apply(len) == 1].copy()
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
    rules[b.A] = rules[b.A].str.join(', ')
    list_of_frozenset = rules[b.B].tolist()
    rules[b.pattern]  = pd.Series([set(f_set) for f_set in list_of_frozenset], index=rules.index)

    # Dùng .tolist() để ép series về list chứa các set, pd.df sẽ tự unpack set thành nhiều cột
    splitted_B = pd.DataFrame(list_of_frozenset, index=rules.index)
    splitted_B.columns = b.B + '_' + splitted_B.columns.astype(str)

    rules = pd.concat([splitted_B, rules.drop(columns=b.B)], axis=1).fillna('-')
    columns = [b.A, b.pattern] + rules.columns.drop([b.A, b.pattern]).tolist()
    return rules[columns]
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
# start_date  = pd.to_datetime('01-01-2026', dayfirst=True)
start_date  = pd.to_datetime('30-06-2025', dayfirst=True)
end_date    = pd.to_datetime('30-06-2026', dayfirst=True)
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
device_mask   = lambda df: ~ df[c.cat].isin(['3RD ACC', 'APPLE ACC'])
device_set    = frozenset(sales.loc[device_mask, c.sku].tolist())
product_map   = sales[[c.sku, c.prod_name]].drop_duplicates(subset=c.sku).set_index(c.sku)[c.prod_name]
product_dict  = product_map.to_dict()
#endregion

#region #? Rules (Third try)
rules = apriori_defined_rules(sales, device_set)
device_rules = rules.loc[rules[b.A].isin(device_set), [b.A, b.pattern]]
device_rules[b.A] = device_rules[b.A].map(product_map).pipe(product_shorten, pattern='device')
group_device = [
    {b.A: Atd, b.pattern: set.union(*Csq)} 
    for Atd, Csq in device_rules.groupby(b.A)[b.pattern]
]

acc_rules = rules.loc[~rules[b.A].isin(device_set), [b.A, b.pattern]]
acc_rules[b.A] = acc_rules[b.A].map(product_map).pipe(product_shorten, pattern='acc')
acc_mask  = ~ acc_rules[b.A].str.contains(r'^\s*$|^\s*/|(?:\s*cable)', case=False, regex=True)
acc_rules = acc_rules[acc_mask]
group_acc = [{b.A: head, b.pattern: g_tail} for head, tail in acc_rules.groupby(b.A)[b.pattern] if len(g_tail := set.union(*tail)) >= 3]

summary, result = st.columns([1, 4], gap='large')
color = "#FFFFFF"
st.html("""
    <style>
    .st-key-device_attachments, .st-key-accessory_bundles {
        background-color: #E8F2FF !important;
        border: 1px solid #FFFFFF !important;
        padding: 2rem;
    </style>
""")
st.html("""
    <style>
    button[data-testid="stPopoverButton"] div[data-testid="stMarkdownContainer"] p {
        font-family: monospace !important;
    }
    </style>
""")
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
            max_str = len(max([d[b.A] for d in group], key=len))
            device_suggestions = st.columns(4, gap='large')
            for idx, subgroup in enumerate(group):
                antecedent = subgroup[b.A]
                cons_sku   = sorted(list(subgroup[b.pattern]))
                cons_name  = list_sku_to_product(cons_sku)
                str_gap    = (max_str - len(antecedent) + 1) * '\u2000'
                if i_key  := [i for i in icon_pack if i in antecedent.upper()]:
                    icon = icon_pack[i_key[0]] if is_icon == 'On' else None
                else:
                    icon = None
                with device_suggestions[idx % 4]:
                    sku_count = len(cons_sku)
                    with st.popover(
                        label   = f'**{antecedent}**{str_gap}| {sku_count:02d}',
                        width   = 'stretch',
                        icon    = icon,
                        type    = 'secondary'
                        ):
                        st.dataframe(
                            pd.DataFrame({'SKU': cons_sku, 'Product Name': cons_name}, index = range(1, sku_count + 1)),
                            height = 'content',
                            width  = 1000
                            )


#endregion

#region Rules (Try 2)
# # Tạo pair_id từ frozenset mặc dù Nhanh hơn nhưng không giữ đc bản chất A, B -> join trước khi tạo id
# # Phải sort để SKU Apple lên đầu khi join
# rules[b.A] = rules[b.A].map(sorted).str.join(', ')
# rules[b.B] = rules[b.B].map(sorted).str.join(', ')
# rules['pair_id'] = rules.apply(lambda r: tuple(sorted([r[b.A], r[b.B]])), axis=1)
# rules = rules.sort_values(by=['pair_id', b.conf], ascending=[True, False])
# rules = rules.drop_duplicates(subset='pair_id', keep='first', ignore_index=True).drop(columns='pair_id')

# not_B_single = rules[b.B].str.contains(',').any()
# splitted_A = rules[b.A].str.split(', ', expand=True)
# splitted_A.columns = b.A + '_' + splitted_A.columns.astype(str)
# rules = pd.concat([splitted_A, rules.drop(columns=b.A)], axis=1)

# # Tạo id lần 2
# subset = rules.loc[:, :b.B].fillna('').astype(str)
# rules['pair_id'] = [' '.join(sorted(row)) for row in subset.values]
# rules = rules.sort_values(by=['pair_id', b.lift], ascending=[True, False])
# # rules = rules.drop_duplicates(subset='pair_id', keep='first', ignore_index=True).drop(columns='pair_id')

# # B should be single after sort and drop (single confidence should always be larger)
# if not_B_single:
#     st.info('B are not single')
#     st.stop()
# # Show product name
# if st.segmented_control('label', options=['Name', 'SKU'], default='SKU', label_visibility='collapsed') == 'Name':
#     rules.loc[:, :b.B] = rules.loc[:, :b.B].apply(lambda col: col.map(product_map))
#endregion
