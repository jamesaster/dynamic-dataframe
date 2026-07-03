# Dynamic Dataframe: Retail Analytics & ML-Driven Operations Suite

![Python](https://img.shields.io/badge/Python-v3.14.3-blue.svg)
![Streamlit](https://img.shields.io/badge/Streamlit-v1.56.0-FF4B4B.svg)
![Streamlit ECharts](https://img.shields.io/badge/Streamlit%20ECharts-v0.6.0-E4393C.svg)
![scikit-learn](https://img.shields.io/badge/scikit--learn-v1.9.0-F7931E.svg?logo=scikit-learn&logoColor=white)
![SciPy](https://img.shields.io/badge/SciPy-v1.18.0-8CAAE6.svg?logo=scipy&logoColor=white)
![statsmodels](https://img.shields.io/badge/statsmodels-v0.14.6-1f4257.svg?logo=python&logoColor=white)
![SHAP](https://img.shields.io/badge/SHAP-v0.52.0-000000.svg)

---

## 💡 The Story Behind

> **"Điều phối luồng dữ liệu để giải quyết triệt để các bài toán vận hành."**
> 
> Khi các DataFrame khô khan không còn đủ để truyền tải insight, tôi quyết định tạo ra một 'hệ điều hành' riêng cho mình bằng Streamlit và ECharts. Tôi muốn mỗi biểu đồ phải có một câu chuyện, một tiếng nói phản ánh thực trạng vận hành. Mỗi chuyển động trong app đều được tinh chỉnh để khớp hoàn hảo với tư duy quản trị mà tôi tích lũy được. Đây không chỉ là công cụ, mà là thành quả của việc cá nhân hóa trải nghiệm dữ liệu, biến những con số vô tri thành lời giải trực diện cho bài toán vận hành.

---

## 📌 Business Case

Vấn đề của các hệ thống quản lý cũ không nằm ở công nghệ, mà là 'rào cản ngăn cách' giữa dòng chảy bán hàng và luân chuyển kho vận. Sự rời rạc này tạo ra những điểm mù khiến vận hành trở nên bị động:

* **Dữ liệu phân mảnh:** `File bán` và `File tồn hệ thống` tồn tại như hai thực thể tách biệt, đầy rẫy nhiễu. Thay vì ghép nối thủ công, tôi đào sâu vào cấu trúc dữ liệu để các mối tương quan tự lộ diện, chuyển hóa dữ liệu thô thành một mặt phẳng thông tin duy nhất.
* **Đứt gãy tầm nhìn:** Sự thiếu liên kết khiến tốc độ tiêu thụ theo từng SKU trở nên mờ mịt. Quyết định nhập hàng thường bị kẹt trong cảm tính thay vì bám sát thực tế sàn bán.
* **Lãng phí nguồn lực:** Đội ngũ chôn vùi thời gian trong các file đối soát thay vì tối ưu trải nghiệm khách hàng.
* **Khát khao Insight:** Bài toán ở đây là chuyển hóa các file thô vô tri thành hệ thống dữ liệu có chiều sâu. Tôi xây dựng ma trận *Demand/Supply*, đồng thời phân tích sâu 14 chỉ số vận hành store trọng yếu để làm rõ ngay lập tức: *mã hàng nào đang là điểm nghẽn, hiệu suất nhân sự đang ở đâu, và những lỗ hổng nào trong dòng chảy hàng hóa cần được tiếp ứng kịp thời.

**Giải pháp:**
Hệ thống này đóng vai trò là "hệ điều hành" cho cửa hàng. Thay vì chỉ hiển thị, nó chủ động làm rõ các mối liên hệ ngầm trong vận hành, biến dữ liệu cồng kềnh thành thông tin hành động. Khi đã làm chủ được sự tương quan, tôi không chỉ tối ưu tồn kho mà còn tái thiết lập hiệu quả làm việc, giải phóng team khỏi gánh nặng báo cáo để tập trung vào giá trị vận hành thực thụ.

---

## 📋 Dashboard

### Khối Chức Năng (UI/UX Breakdown)

    +-------------------------------------------------------------------------+
    | [ KPI Card 1 ]      [ KPI Card 2 ]      [ KPI Card 3 ]   [ KPI Card 4 ] |
    +-----------------------------------------------------+-------------------+
    |                                                     |                   |
    |                  Main Hero View                     |     Finder &      |
    |             (Interactive Big Series)                |   Daily Explore   |
    |                                                     |                   |
    +-----------------------------------------------------+-------------------+
    |                                                     |                   |
    |                  Performance &                      |  Target Tracking  |
    |                Inventory Report                     |                   |
    |                                                     |                   |
    +----------------------------------+--------------------------------------+
    |                                  |                            |         |
    |              Revenue Treemap     |      Stock Movement        |  Metric |
    |                                  |                            |         |
    +----------------------------------+--------------------------------------+

### 🔹 Các Module chức năng chính

* **1. KPI Radar:** 4 chỉ số cốt lõi thể hiện "sức khỏe" cửa hàng, tích hợp Histogram xu hướng để nắm bắt biến động tức thời mà không cần mở file báo cáo.
* **2. Hero Suite & Finder UI:**
    * **Hero View:** Bức tranh toàn cảnh về doanh thu Daily và xu hướng 7 ngày, giúp đưa ra quyết định dựa trên dữ liệu thời gian thực.
    * **Finder:** Công cụ truy vấn dữ liệu gốc (Drill-down). Chỉ với một cú click vào các tham số trọng yếu, tôi có thể lặn sâu vào dữ liệu hóa đơn/khách hàng để tìm nguyên nhân gốc rễ.
* **3. Hiệu suất & Mục tiêu (Performance & Target Control):**
    * **Store/Team Analytics:** Chuẩn hóa dữ liệu theo khung thời gian Daily/Weekly/Monthly, triệt tiêu hoàn toàn sự sai lệch giữa các chu kỳ báo cáo.
    * **Smart Inventory:** Pipeline tự động hóa phân loại. Hệ thống thực hiện Scoring dựa trên *Velocity* và *Supply*.
* **4. Luồng vận động hàng hóa (Core Distribution):**
    * **Revenue Treemap:** Bóc tách tỉ trọng doanh thu theo ngành hàng. Đây là cửa ngõ phân tích, cho phép drill-down để nhận diện ngay lập tức đâu là "key items" và đâu là điểm nghẽn doanh thu.
    * **Stock Flow (Interactive):** Module này được kích hoạt trực tiếp từ hành động chọn ngành hàng trên *Revenue Treemap*. Nó tự động đồng bộ hóa để giám sát biến động Nhập - Xuất - Bán - Trả, kèm đường line lũy kế, giúp tôi kiểm soát chính xác vòng đời của sản phẩm.
---

## 🧪 Analysis (SHAP)
### 🔹 Feature Impact Analysis (ML-Driven Intelligence)
* **Mục tiêu:** Vượt qua giới hạn của các báo cáo tĩnh, module này sử dụng Học máy (`Random Forest` & `SHAP Theory`) như một "kính hiển vi" để bóc tách và định lượng chính xác mức độ đóng góp (+/-) của từng chỉ số trọng yếu lên doanh thu sàn. Từ đó, loại bỏ hoàn toàn cảm tính khi đưa ra quyết định điều phối.
* **Giải pháp kỹ thuật phục vụ quản trị:** * *Kiểm soát cộng tuyến (Multicollinearity):* Áp dụng nghiêm ngập việc sàng lọc biến. Loại bỏ các dữ liệu trùng lặp về mùa vụ để cô lập dòng chảy vào đúng 6 động lực vận hành/thương mại cốt lõi: `Conversion Rate`, `UPT`, `Foot Traffic`, `Promotion`, `Trade-In`, và `Installment`.
    * *Trực quan hóa mật độ (Density Mapping):* Tích hợp thuật toán ước lượng mật độ để phân rã các điểm dữ liệu trên ECharts. Giúp người quản lý nhìn thấy ngay tần suất xuất hiện và khoảng dao động của hiệu quả vận hành.
* **Giá trị thực chiến (Actionable Insights):** Hệ thống không chỉ đưa ra con số mà kết nối thẳng tới các kịch bản hành động (Popover UI) để tối ưu hóa hiệu suất Store:
    * Định vị "điểm gãy" của tỷ lệ chuyển đổi (dưới mốc 3.5%) để kịp thời chấn chỉnh quy trình tiếp cận khách hàng của nhân sự.
    * Nhận diện "ngưỡng trần bão hòa" của UPT (mốc 2.0) để điều chỉnh mục tiêu doanh số bán kèm (Cross-selling) thực tế hơn, tránh gây áp lực vô ích lên đội ngũ.
    * Phân hóa kịch bản Traffic để chủ động lập kế hoạch: Đẩy ưu đãi ngày thường khi khách dưới ngưỡng 500, và chuyển trọng tâm sang điều phối nhân sự/bố trí quầy kệ khi khách vượt ngưỡng cao điểm 2,000.
    * Đo lường biên độ ảnh hưởng và bóc tách hiện tượng "gộp hiệu năng" giữa các chương trình Marketing với giải pháp tài chính (Trade-In, Trả góp), tối ưu hóa cấu trúc chi phí và ngân sách ưu đãi tại store.
---
## 📂 Project Structure
```
.
├── .devcontainer/
│   └── devcontainer.json
├── .streamlit/
│   └── config.toml
├── core/
│   ├── anonymize_sales_product.py
│   ├── finalize_and_clean_dataset.py
│   ├── load_normalize.py
│   ├── mimic_financial_logic.py
│   ├── pipe_run.py
│   ├── process_product_master.py
│   ├── run_auth_pipe.py
│   ├── sales_join_traffics.py
│   ├── secure_customer_identity.py
│   ├── secure_product_identity.py
│   └── validate_standardize.py
├── sections/
│   └── dashboard/
│       ├── get_data.py
│       ├── hero_n_finder.py
│       ├── high_level_df.py
│       ├── kpis_4_metrics.py
│       ├── target_n_pfm.py
│       └── tree_n_movement.py
├── src/
│   ├── columns.py
│   ├── customer_logic.py
│   ├── datetime_logic.py
│   ├── product_logic.py
│   ├── revenue_logic.py
│   ├── stage_n_execute_logic.py
│   ├── stockledger.py
│   └── utils.py
├── views/
│   ├── dashboard.py
│   ├── demo.py
│   ├── shap_analysis.py
│   └── test.py
├── visuals/
│   ├── css_inject.py
│   ├── dynamic_dataframe.py
│   ├── e_charts.py
│   ├── visuals_helper.py
│   └── web_ui.py
├── .gitignore
├── app.py
├── README.md
└── requirements.txt
```

## 🛠️ Requirements

```text
duckdb==1.5.4
google_api_python_client==2.197.0
joblib==1.5.3
numpy==2.5.0
pandas==3.0.3
protobuf==7.35.1
python-dotenv==1.2.2
scikit_learn==1.9.0
scipy==1.18.0
shap==0.52.0
statsmodels==0.14.6
streamlit==1.56.0
streamlit_echarts==0.6.0

