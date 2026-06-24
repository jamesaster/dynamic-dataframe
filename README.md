# Dynamic Dataframe: Retail Analytics & Store Operations Suite

![Python](https://img.shields.io/badge/Python-v3.14.3-blue.svg)
![Streamlit](https://img.shields.io/badge/Streamlit-v1.56.0-FF4B4B.svg)
![Streamlit ECharts](https://img.shields.io/badge/Streamlit%20ECharts-v0.6.0-E4393C.svg)

Dự án tái cấu trúc dữ liệu bán lẻ rời rạc thành một bộ máy vận hành trực quan, biến con số thành thông tin hành động tức thì.

---

## 💡 The Story Behind

> **"Điều phối luồng dữ liệu để giải quyết triệt để các bài toán vận hành."**
> 
> Tôi từng cảm thấy bế tắc khi phải tương tác với các DataFrame khô khan của Pandas/Numpy – nơi thiếu hẳn sự phản hồi trực quan cần thiết. Thay vì chấp nhận, tôi kết hợp Streamlit và ECharts để hiện thực hóa quy trình vận hành. Tôi không chấp nhận những biểu đồ mặc định; mỗi tham số, mỗi chuyển động đều được tôi tinh chỉnh kỹ lưỡng cho đến khi khớp hoàn hảo với tư duy quản trị. Đây không đơn thuần là công cụ trực quan hóa, mà là thành quả của việc cá nhân hóa trải nghiệm, biến dữ liệu thành lời giải trực diện cho những bài toán vận hành tôi từng đối mặt.

---

## 📌 Business Case

Vấn đề của các hệ thống quản lý cũ không nằm ở công nghệ, mà là "bức tường ngăn cách" giữa dòng chảy bán hàng và luân chuyển kho vận. Sự rời rạc này tạo ra những điểm mù khiến vận hành trở nên bị động:

* **Dữ liệu phân mảnh:** `File bán` và `File tồn hệ thống` tồn tại như hai thực thể tách biệt, đầy rẫy nhiễu. Thay vì ghép nối thủ công, tôi đào sâu vào cấu trúc dữ liệu để các mối tương quan tự lộ diện, chuyển hóa dữ liệu thô thành một mặt phẳng thông tin duy nhất.
* **Đứt gãy tầm nhìn:** Sự thiếu liên kết khiến tốc độ tiêu thụ theo từng SKU trở nên mờ mịt. Quyết định nhập hàng thường bị kẹt trong cảm tính thay vì bám sát thực tế sàn bán.
* **Lãng phí nguồn lực:** Đội ngũ chôn vùi thời gian trong các file đối soát thay vì tối ưu trải nghiệm khách hàng.
* **Khát khao Insight:** Bài toán ở đây là chuyển hóa các file thô vô tri thành hệ thống dữ liệu có chiều sâu. Tôi xây dựng ma trận *Demand/Supply*, đồng thời phân tích sâu 14 chỉ số vận hành store trọng yếu để làm rõ ngay lập tức: *mã hàng nào đang là điểm nghẽn, hiệu suất nhân sự đang ở đâu, và những lỗ hổng nào trong dòng chảy hàng hóa cần được tiếp ứng kịp thời.

**Giải pháp:**
Hệ thống này đóng vai trò là "hệ điều hành" cho cửa hàng. Thay vì chỉ hiển thị, nó chủ động làm rõ các mối liên hệ ngầm trong vận hành, biến dữ liệu cồng kềnh thành thông tin hành động. Khi đã làm chủ được sự tương quan, tôi không chỉ tối ưu tồn kho mà còn tái thiết lập hiệu quả làm việc, giải phóng team khỏi gánh nặng báo cáo để tập trung vào giá trị vận hành thực thụ.

---

## 📐 Kiến Trúc Giao Diện & Khối Chức Năng (UI/UX Breakdown)

### 🔹 Sơ đồ bố cục tổng quan

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

## 📂 Cấu Trúc Thư Mục Dự Án (Project Structure)

    .
    ├── .streamlit/           # Cấu hình UI
    ├── sections/             # Logic Dashboard theo phân đoạn chức năng
    ├── src/
    │   ├── columns.py        # Centralized Mapping (Đồng bộ danh định)
    │   └── stockledger.py    # Pipeline: Clean, Pivot, Reshape & Scoring
    ├── visuals/              # Custom ECharts & UI Dynamic
    ├── app.py                # Main Entry Point
    └── requirements.txt      # Dependencies

---

## 🛠️ Yêu Cầu Hệ Thống (Requirements)

```text
duckdb==1.5.4
google_api_python_client==2.197.0
numpy==2.5.0
pandas==3.0.3
plotly==6.8.0
protobuf==7.35.1
python-dotenv==1.2.2
streamlit==1.56.0
streamlit_echarts==0.6.0
