import numpy as np
import pandas as pd
import streamlit as st
import statsmodels.api as sm
from src.columns import colName as c
from sections.dashboard.get_data import load_files_from_drive
from sklearn.ensemble import RandomForestRegressor
from statsmodels.nonparametric.smoothers_lowess import lowess
from streamlit_echarts import st_echarts, JsCode
from sklearn.metrics import mean_absolute_error, r2_score
from scipy.stats import gaussian_kde
from visuals import styled_header
import joblib
import shap
SS = st.session_state
class F:
    is_holiday   = 'Holiday'
    is_npi       = 'NPI iPhone'
    has_promo    = 'Promotion'
    has_ins      = 'Installment'
    has_trade    = 'Trade-In'
    upt          = 'UPT'
    atv          = 'ATV'
    conrate      = 'Conversion Rate'
    peak_2       = 'Peak Days 2%'
    peak_5       = 'Peak Days 5%'
    peak_10      = 'Peak Days 10%'
    rev_m        = 'Revenue (Million)'
    traffic_s    = 'Traffic (Scaled)'
    month        = 'month'
    weekday      = 'is_weekday'
    saturday     = 'Saturday'
    sunday       = 'Sunday'
    head_count   = 'Headcount'
    dof_month    = 'day_of_month'
    dof_week     = 'day_of_week'
    dof_year     = 'day_of_year'

    recent_bias  = 'recent_weight'
    
    traffic      = 'Foot Traffic'
    traffic_nor  = '5d_traffic'
    traffic_sat  = 'sat_traffic'
    traffic_sun  = 'sun_traffic'
    traffic_NPI  = 'traffic_x_NPI'
    performance  = 'traffic * conversion * upt'

    invoice_uniq = 'unique_inv'
    sku_uniq     = 'unique_sku'

# Dashboard mô tả và tối ưu vận hành (Descriptive Analytics)

#region     0. functions
@st.cache_data(show_spinner='Featuring...')
def feature_engineering(raw_sales: pd.DataFrame, aggregate: dict, _def_metrics: dict):
    df_ready = raw_sales.groupby(c.date, as_index=False).agg(**aggregate)
    for col, (func, dtype) in _def_metrics.items():
        df_ready[col] = df_ready.pipe(func).astype(dtype)
    return df_ready

@st.cache_resource(show_spinner='ok')
def get_rf_model(X, y):
    rf_model = RandomForestRegressor(
        n_estimators     = 300,
        max_depth        = 25,     # Optimized
        min_samples_leaf = 5,      # Optimized
        max_features     = 0.5,    # Optimized
        random_state     = 42,
        n_jobs           = -1
    )
    rf_model.fit(X, y)
    joblib.dump(rf_model, r'D:\Python\Dynamic_Dataframe\random_forest.pkl')
    return rf_model

@st.cache_data(show_spinner='Analyzing feature impacts...')
def shap_scatter_config(_model: RandomForestRegressor, X: pd.DataFrame):
    def get_shap_data():
        """
        ## Tạo mô hình giải thích (Explainer) dựa trên rf_model
        - Explainer nhận vào một hoặc nhiều dòng X (Ví dụ: upt=1.6, is_promo=0, is_sunday=1)
        - Explainer tính SHAP value của MỖI giá trị X tương ứng (+|-) bao nhiêu y
        - Ví dụ SHAP upt = +30, is_promo = -15, is_sunday = +35
        """
        Explainer  = shap.TreeExplainer(_model)
        shap_obj   = Explainer(X)

        base_y     = {'base': shap_obj.base_values[0]}
        shap_array = shap_obj.values
        features   = shap_obj.feature_names
        x_axis     = X.round(2)
        y_axis     = pd.DataFrame(shap_array, columns=features).round(2)

        return x_axis, y_axis, base_y
    
    x_axis, y_axis, base = get_shap_data()
    features = y_axis.columns
    scatters = {}

    scatters['col_list'] = []
    for col in features:
        if col in scatter_ignore_features:
            continue
        scatters['col_list'].append(col)
        scatters[col] = {}

        x_vals  = x_axis[col].to_numpy(dtype=float)
        y_vals  = y_axis[col].to_numpy(dtype=float)
        density = gaussian_kde(x_vals)(x_vals)

        if x_axis[col].nunique() <= 15:
            sub_header = 'impact sensitivity'
            x_vals = x_vals + np.random.uniform(-0.05, 0.05, size=x_vals.shape)
        else:
            sub_header = ''

        matrix  = np.column_stack((x_vals, y_vals, density))
        matrix_sorted = matrix[matrix[:, 2].argsort()]
        min_density   = np.min(density).item()
        max_density   = np.max(density).item()

        max_x = np.percentile(x_vals, 99.5).item()
        min_x = None

        if max_x <= 1.05:
            max_x = 1.5
            min_x = -0.5
        else:
            max_x = int(np.ceil(max_x))
        
        scatters[col]['dataset'] = matrix_sorted.tolist()
        scatters[col]['min_den'] = min_density
        scatters[col]['max_den'] = max_density
        scatters[col]['max_x']   = max_x
        scatters[col]['min_x']   = min_x
        scatters[col]['sub_header']  = sub_header  

    return base | scatters

@st.fragment
def shap_scatter_chart(
    data    : dict,
    feature : str = 'upt',
    unit    : str = 'VND',
    height  : int = 600,
    color_idx : int = 0
):
    scatter_data = data[feature]['dataset']
    min_density  = data[feature]['min_den']
    max_density  = data[feature]['max_den']
    max_x        = data[feature]['max_x']
    min_x        = data[feature]['min_x']
    is_bool      = True if data[feature]['sub_header'] else False
    colors = [
        "#4D79A7",
        "#59A14F",
        "#4E9E9C",
        "#9C755F",
        "#7368B9",
        "#DD6A94",
        "#A0CBE8",
        "#7AB56D",
        "#86BCB6",
        "#F28E2B",
        "#E15759",
        "#B07AA1"
    ]
    spare_idx    = (color_idx + 2) % len(colors)
    sparse_color = colors[spare_idx] if is_bool else "#BBBBBB"
    dense_idx    = color_idx % len(colors)
    dense_color  = colors[dense_idx]
    base_revenue = 365
    base_color   = "#FF9940"
    tooltip_js = f"""
        function (p) {{
            // Kiểm tra nếu dữ liệu không tồn tại, undefined hoặc vô cực =>> markLine
            if (!p.data || p.data[0] === undefined || p.data[1] === undefined || !isFinite(p.data[0]) || !isFinite(p.data[1])) {{
                return `
                    <div style="font-size:14px; font-weight:600; line-height:1.5; padding: 2px 5px;">
                        <span style="color: #6880AA;">Base Revenue:</span> 
                        <b style="color: {base_color};">{base_revenue} {unit}</b>
                    </div>
                `;
            }}

            let val = Math.abs(p.data[1]).toLocaleString(undefined, {{minimumFractionDigits: 1, maximumFractionDigits: 1}}) + ' {unit}';
            let text = p.data[1] >= 0 ? `<span style="color:#1890ff;">+${{val}}</span>` : `<span style="color:#CE3C32;">-${{val}}</span>`;
            return `
                <div style=" font-size:14px; font-weight:600; line-height:1.5;">
                    <div style="border-bottom:1px solid #eee; margin-bottom:5px; font-weight:bold;">${{p.seriesName}}</div>
                    <div style="display:flex; justify-content:space-between; gap:20px;">
                        <span>{feature}:</span>
                        <b>${{ { "Math.round(p.data[0]) === 1 ? 'True' : 'False'" if is_bool else "p.data[0]" } }}</b>
                    </div>
                    <div style="display:flex; justify-content:space-between; gap:20px;"><span>Impact:</span>${{text}}</div>
                </div>
            `;
        }}
        """

    y_label_formatter = """
        function(value) {
            var absVal = Math.abs(value);
            var sign = value < 0 ? '-' : '';
            if (absVal >= 1000000000) return sign + parseFloat((absVal / 1000000000).toFixed(1)) + 'B';
            if (absVal >= 1000000) return sign + parseFloat((absVal / 1000000).toFixed(1)) + 'M';
            if (absVal >= 1000) return sign + parseFloat((absVal / 1000).toFixed(1)) + 'k';
            return value % 1 === 0 ? value.toLocaleString() : value.toFixed(1);
        }
    """
    
    options = {
        "animation": True,
        "animationDuration": 400,
        "backgroundColor": "white",
        "legend": {
            "show": False,
            "data": ["Observations", "Trend Curve"],
            "top": 5,
            "icon": "circle",
            "textStyle": {"fontSize": 12},
        },
        "tooltip": {
            "trigger": "item",
            "confine": True,
            "backgroundColor": "rgba(255, 255, 255, 0.95)",
            "formatter": JsCode(tooltip_js),
        },
        "visualMap": {
            "show": False,
            "type": "continuous",
            "min": min_density,
            "max": max_density,
            "dimension": 2, # cột 3 = dimension
            "seriesIndex": 0,
            "inRange": {
                "color":  [sparse_color, dense_color],
                "symbolSize": [6, 4.5]
            }
        },
        "grid": {
            "left": "0%",
            "right": "10%",
            "bottom": "5%",
            "top": "5%",
            "containLabel": False,
        },
        "xAxis": {
            "type": "value",
            "min": min_x,
            "max": max_x,
            "name": feature,
            "nameLocation": "middle",
            "nameGap": 35,
            "nameTextStyle": {
                "color": dense_color,
                "fontSize": 14,
                "fontWeight": 600
            },
            "axisLabel": {"color": "#999", "fontSize": 11},
            "axisTick": {"show": False},
            "axisLine": {"show": True},
            "splitLine": {"show": True, "lineStyle": {"type": "dashed", "color": "#eee"}},
        },
        "yAxis": {
            "type": "value",
            "name": f"Effect ({unit})",
            "nameGap": 30,
            "nameTextStyle": {
                "color": "#B0B0B0",
                "fontSize": 12.5,
                "fontWeight": 500
            },
            "axisLabel": {
                "color": "#999",
                "fontSize": 11,
                "formatter": JsCode(y_label_formatter),
            },
            "axisLine": {"show": False, "onZero": False}, # onZero = label không đi theo mốc 0 trên trục X
            "axisTick": {"show": False},
            "splitLine": {
                "lineStyle": {"type": "dashed", "color": "rgba(0,0,0,0.075)"}
            },
        },
        "series": [
            {
                "name": "Observations",
                "type": "scatter",
                "data": scatter_data,
                "itemStyle": {
                    "opacity": 0.5,
                },
                "emphasis": {
                    "itemStyle": {
                        "opacity": 0.8,
                        "borderColor": dense_color,
                        "borderWidth": 25,
                    }
                },
                "markLine": {
                    "data": [{"yAxis": 0}],
                    "animation": False,
                    "silent": False,
                    "symbol": ["diamond", "diamond"],
                    "symbolSize": 6,
                    "z": 0,
                    "label": {
                        "position": "end",
                        "formatter": " Base",
                        "color": "#B0B0B0",
                        "fontSize": 12.5,
                        "fontWeight": 500
                    },
                    "lineStyle": {
                        "type": [6, 4],
                        "color": base_color,
                        "opacity": 0.95,
                        "width": 2.0
                    },
                    "emphasis": {
                        "lineStyle": {
                            "type": [7, 4],
                            "color": base_color,
                            "opacity": 1,
                            "width": 2.2,
                        }
                    }
                }
            }
        ]
    }
    st_echarts(options=options, height=f'{height}px', key=f'shap_scatter_{feature}')


def evaluate_model(
    model, col_X, X_train, y_train, X_test, y_test, unit="triệu VND"
):
    """Hàm tối ưu hóa: Tự động lọc cột col_X, tính toán và in báo cáo hiệu năng chống Overfitting."""
    # SỬA LỖI CHÍ MẠNG: Ép mô hình chỉ được dự báo trên đúng danh sách biến col_X được chọn
    X_train_filtered = X_train[col_X]
    X_test_filtered = X_test[col_X]

    # Dự báo dựa trên tập dữ liệu sạch nhiễu
    y_train_pred = model.predict(X_train_filtered)
    y_test_pred = model.predict(X_test_filtered)

    # Tính toán chỉ số độc lập
    r2_train = r2_score(y_train, y_train_pred)
    r2_test = r2_score(y_test, y_test_pred)

    mae_train = mean_absolute_error(y_train, y_train_pred)
    mae_test = mean_absolute_error(y_test, y_test_pred)

    # Tính khoảng cách lệch để cảnh báo chính xác bằng số
    r2_gap = r2_train - r2_test

    print("\n=================== MODEL PERFORMANCE ===================")
    print(f"Features ({len(col_X)} cols): {col_X}")
    print("-" * 57)
    print(f"1. R-squared (R2) - Độ giải thích phương sai:")
    print(f"   - Tập TRAIN : {r2_train:.4f} (Độ khớp dữ liệu quá khứ)")
    print(f"   - Tập TEST  : {r2_test:.4f} (Độ tổng quát hóa thực tế)")
    print("-" * 57)
    print(f"2. Mean Absolute Error (MAE) - Sai số tuyệt đối trung bình:")
    print(
        f"   - Tập TRAIN : {mae_train:.2f} {unit} (Lệch trung bình khi học)"
    )
    print(f"   - Tập TEST  : {mae_test:.2f} {unit} (Lệch thực tế khi kiểm tra)")
    print("=========================================================")

    # Logic cảnh báo
    if r2_gap > 0.15:
        print(
            f"⚠️ LƯU Ý: Khoảng cách R2 giữa Train và Test quá lớn ({r2_gap*100:.1f}%). Mô hình bị Overfitting nặng!"
        )
    elif r2_gap < -0.05:
        print(
            f"❓ CẢNH BÁO: R2 Test cao hơn hẳn Train ({abs(r2_gap)*100:.1f}%). Hãy kiểm tra lại xem tập Test có bị quá nhỏ hoặc trùng lặp dữ liệu không!"
        )
    else:
        print(
            f"✅ THÀNH CÔNG: Khoảng cách R2 ổn định ({r2_gap*100:.1f}%). Mô hình đạt độ tổng quát hóa an toàn."
        )


def tune_random_forest_grid(X_train, y_train, X_test, y_test, grid=None):
    """
    Hàm chạy loop thử nghiệm hyperparameter cho RandomForestRegressor,
    in ra kết quả trực tiếp và trả về bộ tham số tối ưu nhất dựa trên R2 Test.
    """
    import itertools
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.metrics import r2_score, mean_absolute_error

    # Cấu hình grid mặc định nếu không truyền vào
    if grid is None:
        grid = {
            'max_depth':20,
            'min_samples_leaf':10,
            'max_features': [1.0, 0.5, 'sqrt'],
        }

    best_r2 = -float('inf')
    best_params = None

    keys, values = zip(*grid.items())
    for v in itertools.product(*values):
        params = dict(zip(keys, v))
        
        test_model = RandomForestRegressor(
            n_estimators=300,
            random_state=42,
            n_jobs=-1,
            **params
        )
        test_model.fit(X_train, y_train)
        
        # Tính toán nhanh chỉ số trên tập TEST
        y_pred_test = test_model.predict(X_test)
        r2_t = r2_score(y_test, y_pred_test)
        mae_t = mean_absolute_error(y_test, y_pred_test)
        
        print(f"Params: {params} -> R2 Test: {r2_t:.4f} | MAE Test: {mae_t:.2f}M")
        
        # Lưu lại bộ tham số tốt nhất
        if r2_t > best_r2:
            best_r2 = r2_t
            best_params = params

    print(f"\n[BEST] Params: {best_params} với R2 Test: {best_r2:.4f}")
    return best_params
#endregion

#region     1. Theme
Language = {
    'VI': {
        'title'     : '🧪 Feature Impact Analysis',
        'sub'       : """
            <div style="font-size: 0.85rem; color: #6880AA; line-height: 1.5; margin-bottom: 35px; margin-top: 0px;">
            <span style="font-size: 1.15rem; font-weight: 800; color: #6880AA; vertical-align: -1px; margin-right: 4px;">SHAP</span> 
            là phương pháp giải thích mô hình học máy dựa trên <strong>lý thuyết trò chơi về sự hợp tác (Game Theory)</strong>.
            <br>
            Công cụ này phân tích mức độ đóng góp và hướng tác động của từng <strong>nhân tố (feature)</strong> đến kết quả.
            <br>
            <div style="font-size: 0.75rem; font-style: normal; color: #8AA0C4; margin-top: 5px;">
            * Dữ liệu trong dự án này được giả lập dựa trên xu hướng vận hành thực tế.
            </div>
            </div> """,
        'sb_title'  : 'Biểu đồ nói lên điều gì?',
        'sb_guide'  : """
            Mỗi biểu đồ là một **bản đồ mô phỏng kịch bản vận hành** từ dữ liệu lịch sử, giúp bạn tự động nhận diện quy luật mà không cần rà soát thủ công.

            ---
            ### 📐 Ý nghĩa các trục:
            * **Trục ngang (Chỉ số):** Dải giá trị ghi nhận thực tế (hoặc đại diện cho trạng thái Có / Không).
            * **Trục dọc (Mức tác động):** Mức tăng trưởng hoặc sụt giảm so với doanh thu trung bình (Baseline).
            ---
            ### 🫧 Ý nghĩa vị trí các điểm dữ liệu:
            * **Trên Baseline:** Tác động tích cực, kéo doanh thu ngày đi lên.
            * **Tại Baseline (Đường màu cam):** Ngày vận hành ổn định, doanh thu duy trì ở mức nền cơ sở.
            * **Dưới Baseline:** Không tạo hiệu ứng tích cực hoặc kéo doanh thu ngày đi xuống.
            ---
            *Mật độ các chấm cao đại diện cho vùng hội tụ của những ngày mang tính chất điển hình, phản ánh mô hình hành vi chung của hệ thống trong phần lớn thời gian.*
            """,
        'feature_info'   : {
            F.conrate   : {
                'insight': f'Doanh thu dưới sàn tại <= 3.5%, tăng mạnh nhưng thưa dần từ 5.5%',
                'action': 'Nhận diện vùng trên **5.5%** còn rất nhiều dư địa tăng trưởng. Cần tập trung tìm hiểu nguyên nhân, duy trì và khai thác tiếp vùng doanh thu cao này. '
                        'Đồng thời, chủ động cải thiện hiệu suất các ngày có tỷ lệ dưới **3.5%** để đưa doanh thu về lại mức kỳ vọng.',
                'icon': ':material/shopping_cart_checkout:'
            },
            F.upt       : {
                'insight': 'Tác động tích cực từ 1.5, có dấu hiệu bão hòa và giảm mật độ từ 2.0',
                'action': 'Tập trung tối ưu và duy trì UPT trong khoảng mục tiêu **1.5 - 1.7** để đạt hiệu quả doanh thu cao nhất. '
                        'Không cần thiết phải ép chỉ số vượt mốc 2.0 vì sức mua phụ kiện đã chạm trần bão hòa và tần suất đạt được rất thấp.',
                'icon': ':material/shopping_bag:'
            },
            F.traffic   : {
                'insight': 'Phân hóa tại 500 đại diện cho ngày thường tỉ lệ với cuối tuần, ngày lễ',
                'action': 'Tập trung đẩy mạnh các chương trình khuyến mãi (Promotion) chuyên biệt cho ngày thường để chủ động kéo lượng khách vượt ngưỡng **500**. Đối với các ngày cao điểm (trên **2,000** khách), chuyển trọng tâm sang điều phối vận hành để khai thác trọn vẹn sức mua.',
                'icon': ':material/groups:'
            },
            F.has_promo : {
                'insight': 'Khi có khuyến mãi, hiệu quả bị phân hóa thành hai nhóm',
                'action': 'Rà soát lại danh mục để loại bỏ các chương trình thuộc nhóm hiệu quả thấp **(~20 triệu)**.'
                    'Tập trung ngân sách cho nhóm thiết kế ưu đãi phía trên để tối đa hóa biên độ tác động doanh thu **(+40 triệu)**.',
                'icon': ':material/local_offer:'
            },
            F.has_trade : {
                'insight': 'Tác động tốt nhưng biên độ hiệu quả dao động lớn và thưa thớt',
                'action': 'Duy trì **Trade-In** như một giải pháp gia tăng doanh thu chắc chắn. Cần chuẩn hóa quy trình định giá và kịch bản tư vấn tại quầy để thu hẹp khoảng cách hiệu quả, đẩy các ngày thấp (+5 triệu) lên mức tối ưu hơn.'
                        '\n- **Lưu ý**: Mức tác động hiển thị trên biểu đồ có vẻ thấp do một phần hiệu quả thực tế đã bị tính gộp vào yếu tố **Promotion**.',
                'icon': ':material/published_with_changes:'
            },
            F.has_ins   : {
                'insight': 'Mang lại tác động tốt nhưng mức độ còn thấp và mật độ thưa thớt',
                'action': 'Áp dụng cơ chế thưởng nóng trên mỗi giao dịch trả góp để khuyến khích nhân viên chủ động tư vấn nhằm tăng tần suất chốt đơn. '
                        '\n- **Lưu ý**: Mức tác động hiển thị trên biểu đồ có vẻ thấp do một phần hiệu quả thực tế đã bị tính gộp vào yếu tố **Promotion**.',
                'icon': ':material/payments:'
            },
        }
    },
    'EN': {
        'title'     : '🧪 Feature Impact Analysis',
        'sub'       : """
            <div style="font-size: 0.85rem; color: #6880AA; line-height: 1.5; margin-bottom: 35px; margin-top: 0px;">
            <span style="font-size: 1.15rem; font-weight: 800; color: #6880AA; vertical-align: -1px; margin-right: 4px;">SHAP</span> 
            is a method for explaining machine learning models based on <strong>cooperative Game Theory</strong>.
            <br>
            This tool analyzes the contribution level and impact direction of each <strong>feature</strong> on the final outcome.
            <br>
            <div style="font-size: 0.75rem; font-style: normal; color: #8AA0C4; margin-top: 5px;">
            * Data in this project is simulated based on actual operational trends.
            </div>
            </div> """,
        'sb_title'  : 'What these charts tell?',
        'sb_guide'  : """
            Each chart is an **operational scenario simulation map** derived from historical data, helping you automatically identify patterns without manual review.

            ---
            ### 📐 Axis Meanings:
            * **Horizontal Axis (Metric):** The range of actual recorded values (or representing a Yes / No state).
            * **Vertical Axis (Impact Level):** The growth or decline relative to the average revenue (Baseline).
            ---
            ### 🫧 Data Point Position Meanings:
            * **Above Baseline:** Positive impact, driving daily revenue up.
            * **At Baseline (Orange Line):** Stable operation day, revenue maintained at the baseline level.
            * **Below Baseline:** No positive effect or pulling daily revenue down.
            ---
            *A high density of dots represents the convergence zone of typical days, reflecting the general behavior pattern of the system most of the time.*
            """,
        'feature_info'   : {
            F.conrate   : {
                'insight': 'Revenue bottoms at <= 3.5%, increases sharply but thins out from 5.5%',
                'action': 'Identify the zone above **5.5%** as having substantial growth potential. Focus on analyzing root causes to sustain and exploit this high-revenue zone. '
                        'Concurrently, proactively improve performance on days below **3.5%** to bring revenue back up to expectations.',
                'icon': ':material/shopping_cart_checkout:'
            },
            F.upt       : {
                'insight': 'Positive impact from 1.5, sign of saturation and lower density from 2.0',
                'action': 'Focus on optimizing and maintaining UPT within the target range of **1.5 - 1.7** to maximize revenue efficiency. '
                        'It is unnecessary to push the metric past 2.0 since accessory purchasing power has hit a ceiling and the achievement frequency is very low.',
                'icon': ':material/shopping_bag:'
            },
            F.traffic   : {
                'insight': 'Divergence at 500 represents weekdays vs. weekends and holidays',
                'action': 'Focus on boosting specialized weekday promotions to proactively drive traffic past the **500** threshold. For peak days (over **2,000** visitors), shift focus to operational coordination to fully capture purchasing power.',
                'icon': ':material/groups:'
            },
            F.has_promo : {
                'insight': 'When promotions active, performance splits into two distinct groups',
                'action': 'Review the portfolio to eliminate low-performing campaigns **(~20M)**.'
                    'Concentrate the budget on the upper-tier offer designs to maximize revenue impact margins **(+40M)**.',
                'icon': ':material/local_offer:'
            },
            F.has_trade : {
                'insight': 'Good impact, but performance margins fluctuate widely and sparsely',
                'action': 'Maintain **Trade-In** as a reliable revenue-driving solution. Standardize valuation processes and counter-consultation scripts to narrow the performance gap, lifting low-impact days (+5M) to a more optimal level.'
                        '\n- **Note**: The impact shown on the chart may appear low because a portion of its actual effect has been aggregated into the **Promotion** factor.',
                'icon': ':material/published_with_changes:'
            },
            F.has_ins   : {
                'insight': 'Delivers good impact, but magnitude remains low and density is sparse',
                'action': 'Implement instant cash incentives per installment transaction to motivate staff to proactively consult and increase closing frequencies.'
                        '\n- **Note**: The impact shown on the chart may appear low because a portion of its actual effect has been aggregated into the **Promotion** factor.',
                'icon': ':material/payments:'
            },
        }
    }
}
with st.sidebar:
    mode = st.segmented_control('Language', ['EN', 'VI'], width='stretch', default='VI', label_visibility='collapsed')
    st.title(Language[mode]['sb_title'])
    st.caption(Language[mode]['sb_guide'])
st.title(Language[mode]['title'])
st.markdown(Language[mode]['sub'], unsafe_allow_html=True)
_note = """
#! Hệ số là chung cho cả bảng, không có hệ số riêng cho từng ngày
#? Công thức: Doanh thu = (Intercept * 1) + (Slope * Input variable)
    Intercept - Hệ số chặn: Doanh thu mặc định khi không có yếu tố can thiệp vào vận hành (cùng đơn vị với doanh thu)
    - Ví dụ Intercept = 200Tr (Doanh thu sàn khi không có yếu tố đặc biệt)
    Slope - Hế số góc: (Hệ số) Tỉ lệ hiệu quả của yếu tố can thiệp (traffic, m², Cái, lượt click...)
    - Ví dụ Slope_traffic = 112,000 -> cứ thêm 1 traffic, doanh thu tăng 112k
#! 2 hệ số Intercept, Slope được thuật toán tự tính ra (không cần đào sâu)

#? P-value: Metric độ "ĂN MAY" (Xác suất kết quả bị sai/ngẫu nhiên)
    - P-value chạy từ 0 đến 1 (tương ứng 0% đến 100%).
    - P-value CÀNG NHỎ càng tốt (nghĩa là tỷ lệ ăn may càng thấp -> độ tin cậy càng cao).

TÊN BIẾN             COEF (HỆ SỐ)
──────────────────────────────────────────────────────────
const            │   1.641e+08  <─── ĐÂY LÀ INTERCEPT (Hệ số chặn)
date_traffic     │   1.076e+05  <─── ĐÂY LÀ SLOPE 1 (Hệ số góc của Khách)
is_event         │   7.930e+07  <─── ĐÂY LÀ SLOPE 2 (Hệ số góc của Sự kiện)

#? Luật X: ĐÃ GỌI LÀ "BIẾN ĐỘC LẬP" THÌ PHẢI ĐỘC LẬP THẬT SỰ!
    - Các biến X chỉ được phép tương tác với Y, TUYỆT ĐỐI không được tương tác mạnh với nhau.
      => Biện pháp 1: Chọn ĐẠI DIỆN 1 thằng, vứt thằng còn lại ra ngoài.
      => Biện pháp 2: Nhóm chúng lại thành 1 biến duy nhất (Phép nhân Tương Tác).
"""
#endregion

#region     2. Get sales data
start_date  = pd.to_datetime('01-04-2024', dayfirst=True)
end_date    = pd.to_datetime('30-06-2026', dayfirst=True)
date_mask   = lambda df: df[c.date].between(start_date, end_date)

sales: pd.DataFrame = SS.get('analysis_sales', None)
if sales is None:
    st.info('Switch to dashboard then switch back.')
    st.stop()

raw_sales = sales.loc[date_mask, :]
raw_sales.loc[:, c.event_name] = raw_sales[c.event_name].astype('string').replace('-', pd.NA)
raw_sales = raw_sales[raw_sales[c.revenue] > 0].reset_index(drop=True)
#endregion

#region     3. Aggregate config & Feature Engineering
NPI_DAYS     = ['27-09-2024', '19-09-2025']
aggregate    = {
    c.revenue    : (c.revenue,       'sum'),
    c.qty        : (c.qty,           'sum'),
    c.traffic    : (c.traffic,       'first'),

    F.is_holiday : (c.event_name,    'any'),
    F.has_promo  : ('mkt_promo',     'sum'),
    F.has_ins    : ('payoo',         'sum'),
    F.has_trade  : ('trade_in',      'sum'),
    F.head_count : (c.staff,         'nunique'),
    
    F.invoice_uniq : (c.invoice,       'nunique'),
    F.sku_uniq     : (c.sku,           'nunique'),
}
def_metrics  = {
    F.conrate     : (lambda df: (df[F.invoice_uniq] / df[c.traffic]) * 100, 'float64'),
    F.upt         : (lambda df: df[c.qty] / df[F.invoice_uniq], 'float64'),
    F.atv         : (lambda df: df[c.revenue] / df[F.invoice_uniq], 'float64'),
    F.traffic     : (lambda df: df[c.traffic], 'int'),
    
    F.has_promo   : (lambda df: df[F.has_promo] > 0, 'int8'),
    F.has_trade   : (lambda df: df[F.has_trade] > 0, 'int8'),
    F.has_ins     : (lambda df: df[F.has_ins] > 0, 'int8'),
    F.is_holiday  : (lambda df: df[F.is_holiday] > 0, 'int8'),

    F.month       : (lambda df: df[c.date].dt.month, 'int8'),
    F.dof_week    : (lambda df: df[c.date].dt.day_of_week, 'int8'),
    F.dof_month   : (lambda df: df[c.date].dt.day, 'int8'), 
    F.dof_year    : (lambda df: df[c.date].dt.day_of_year, 'int16'),


    F.is_npi      : (lambda df: df[c.date].isin([pd.Timestamp(d) for d in NPI_DAYS]), 'int8'),
    F.traffic_s   : (lambda df, t = c.traffic: (df[t] - df[t].min()) / (df[t].max() - df[t].min()), 'float64'),
    F.saturday    : (lambda df: df[c.date].dt.weekday == 5, 'int8'),
    F.sunday      : (lambda df: df[c.date].dt.weekday == 6, 'int8'),
    F.traffic_sat : (lambda df: df[F.saturday] * df[F.traffic_s], 'float64'),
    F.traffic_sun : (lambda df: df[F.sunday] * df[F.traffic_s], 'float64'),
    F.traffic_nor : (lambda df: df[F.traffic_s] - df[F.traffic_sat] - df[F.traffic_sun], 'float64'),


    F.recent_bias : (lambda df: np.linspace(0.8, 1.0, len(df)), 'float64'),
    F.rev_m       : (lambda df: df[c.revenue] / 1_000_000, 'float64'),
    F.peak_2      : (lambda df: (df[F.rev_m] >= df[F.rev_m].quantile(q=0.98)) & (~df[F.is_npi]), 'int8'),
    F.peak_5      : (lambda df: ((df[F.rev_m] >= df[F.rev_m].quantile(q=0.95)) 
                                 & (df[F.rev_m] < df[F.rev_m].quantile(q=0.98)) 
                                 & (~df[F.is_npi])), 'int8'),
    F.performance : (lambda df: df[F.traffic_s] * df[F.upt] * df[F.conrate], 'float64'),
}
df_ready     = feature_engineering(raw_sales, aggregate, def_metrics)
#endregion

#region     4. Feature selecting
X_add        = [F.head_count]
common_drop  = [
    F.rev_m,
    F.atv,            # Hệ quả trực tiếp từ công thức toán học (Revenue = Bills * UPT * ATV)
    F.month,          # Gây trùng lặp thông tin mùa vụ/thời gian
    F.dof_week,       # Đã có F.saturday và F.sunday gánh
    F.dof_month,      # Nhiễu, không mang ý nghĩa thống kê trong tập dữ liệu này
    F.dof_year,       # Nhiễu chu kỳ lớn
    F.is_holiday      # Vô nghĩa
]
explain_drop = [
    F.performance,
    F.is_holiday,     # P-value quá cao (0.666), không có ý nghĩa giải thích
    F.traffic_sat,    # Cộng tuyến Traffic
    F.traffic_sun,    # Cộng tuyến Traffic
    F.traffic_nor,    # Cộng tuyến Traffic
    F.recent_bias,    # Không có ý nghĩa
    F.traffic_s # bỏ để thử traffic raw
]
col_X = [col for col in list(def_metrics) + X_add if not col in [*explain_drop, *common_drop]]
col_y = F.rev_m

# anchor = int(len(df_ready) * 0.8)
# X_train = df_ready.iloc[:anchor][col_X]
# X_test  = df_ready.iloc[anchor:][col_X]
# y_train = df_ready.iloc[:anchor][col_y]
# y_test  = df_ready.iloc[anchor:][col_y]

X_full  = df_ready[col_X]
y_full  = df_ready[col_y]
#endregion

#region     #* Random Forest + UI
# rf_model = get_rf_model(X_full, y_full)
# evaluate_model(rf_model, col_X, X_train, y_train, X_test, y_test)
model_pkl = load_files_from_drive()['random_forest.pkl']
rf_model  = joblib.load(model_pkl)

scatter_ignore_features = [F.is_npi, F.peak_2, F.peak_5, F.saturday, F.sunday, F.head_count]
scatters = shap_scatter_config(rf_model, X_full)

f_infos = Language[mode]['feature_info']
st_cols = st.columns(3, gap='large')

for idx, col in enumerate(scatters['col_list']):
    with st_cols[idx % 3]:
        styled_header(col, scatters[col]['sub_header'])

        with st.popover(f_infos[col]['insight'], width='stretch', type='secondary', icon=f_infos[col]['icon']):
            st.info(f_infos[col]['action'], icon='💡')
            
        shap_scatter_chart(data=scatters, feature=col, color_idx=idx, unit='Million VND')
        st.space()
#endregion

#region     #! OLS Linear Regression
# X_train_with_const = sm.add_constant(X_train)
# X_test_with_const  = sm.add_constant(X_test)
# ols_model = sm.OLS(y_train, X_train_with_const).fit()
# print(ols_model.summary())
#endregion

