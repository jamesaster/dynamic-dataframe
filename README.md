# Dynamic Dataframe: Retail Analytics & ML-Driven Operations Suite

![Python](https://img.shields.io/badge/Python-v3.14.3-blue.svg)
![Streamlit](https://img.shields.io/badge/Streamlit-v1.56.0-FF4B4B.svg)
![Streamlit ECharts](https://img.shields.io/badge/Streamlit%20ECharts-v0.6.0-E4393C.svg)
![scikit-learn](https://img.shields.io/badge/scikit--learn-v1.9.0-F7931E.svg?logo=scikit-learn&logoColor=white)
![SciPy](https://img.shields.io/badge/SciPy-v1.18.0-8CAAE6.svg?logo=scipy&logoColor=white)
![statsmodels](https://img.shields.io/badge/statsmodels-v0.14.6-1f4257.svg?logo=python&logoColor=white)
![SHAP](https://img.shields.io/badge/SHAP-v0.52.0-000000.svg)

A retail analytics dashboard that connects sales and inventory data most systems keep siloed — built to surface which SKUs are bottlenecks, where staff performance stands, and where the product flow needs reinforcement, without manual reconciliation between reports.

## Dashboard

![Dashboard](image/home.png)

- **KPI Radar** — 4 core health metrics with trend histograms for instant read, no need to open raw reports.
- **Hero View & Finder** — daily revenue overview with 7-day trend, plus a drill-down tool for invoice/customer-level root-cause queries.
- **Store/Team Analytics** — Daily/Weekly/Monthly standardized comparisons, no cross-cycle discrepancies.
- **Smart Inventory** — automated SKU classification scored on *Velocity* and *Supply*.
- **Revenue Treemap** — revenue share by category; drill-down entry point for finding key items and bottlenecks.
- **Stock Flow** — interactive, driven by Treemap selection; tracks Inbound/Outbound/Sales/Returns with a cumulative line for product lifecycle.

## Analysis (SHAP)

![SHAP Analysis](image/shap_analysis.png)

Uses `Random Forest` + `SHAP` to quantify each metric's actual (+/-) contribution to revenue, replacing intuition-based calls with measured impact.

- **6 core drivers** after multicollinearity screening: Conversion Rate, UPT, Foot Traffic, Promotion, Trade-In, Installment.
- **Density mapping** on ECharts shows the frequency and spread of operational efficiency, not just averages.
- **Actionable thresholds surfaced via Popover UI**, e.g.:
  - Conversion rate below 3.5% → flag staff engagement workflow.
  - UPT saturating around 2.0 → cap cross-sell targets, avoid pushing the team past a ceiling.
  - Traffic under 500 → weekday promo; traffic over 2,000 → shift focus to staffing/shelf layout.
  - Separates compounding effects between marketing campaigns and financial levers (Trade-In, Installment) for cleaner budget allocation.

## Project Structure
```
.
├── .devcontainer/
│ └── devcontainer.json
├── .streamlit/
│ └── config.toml
├── core/
│ ├── anonymize_sales_product.py
│ ├── finalize_and_clean_dataset.py
│ ├── load_normalize.py
│ ├── mimic_financial_logic.py
│ ├── pipe_run.py
│ ├── process_product_master.py
│ ├── run_auth_pipe.py
│ ├── sales_join_traffics.py
│ ├── secure_customer_identity.py
│ ├── secure_product_identity.py
│ └── validate_standardize.py
├── sections/
│ └── dashboard/
│ ├── get_data.py
│ ├── hero_n_finder.py
│ ├── high_level_df.py
│ ├── kpis_4_metrics.py
│ ├── target_n_pfm.py
│ └── tree_n_movement.py
├── src/
│ ├── columns.py
│ ├── customer_logic.py
│ ├── datetime_logic.py
│ ├── product_logic.py
│ ├── revenue_logic.py
│ ├── stage_n_execute_logic.py
│ ├── stockledger.py
│ └── utils.py
├── views/
│ ├── dashboard.py
│ ├── demo.py
│ ├── shap_analysis.py
│ └── test.py
├── visuals/
│ ├── css_inject.py
│ ├── dynamic_dataframe.py
│ ├── e_charts.py
│ ├── visuals_helper.py
│ └── web_ui.py
├── .gitignore
├── app.py
├── README.md
└── requirements.txt
```
## Requirements

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
```
