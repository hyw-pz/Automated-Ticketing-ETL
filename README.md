# Ticketing Data Cleaning & Marketing Report Scripts

## 📝 Project Description
This repository contains a set of R scripts and Quarto reports I developed during my internship to automate ticketing data reconciliation and marketing performance tracking for major musical theater productions (e.g., *Matilda the Musical*, *Next to Normal*).

In the musical theater industry, sales data is highly fragmented across 30+ sources, including direct sales systems, ticketing platforms (Damai, Maoyan), and various venue-side reports. These tools replace manual Excel work with an **automated data pipeline**.

## 🛠 Key Features & Workflows

### 1. Multi-Source ETL (Extract, Transform, Load)
* **Heterogeneous Data Ingestion**: Automated parsing of reports from venue partners (e.g., Shenzhen Poly Theatre, Shanghai Conservatory Opera House) and ticketing giants like Maoyan.
* **Smart Business Logic**: Built-in functions to decode complex ticketing tiers (e.g., "Buy 2 Get 1 Free" packages) and standardize inconsistent date/time strings into ISO formats.
* **Resilient Data Patching**: Designed mechanisms to backfill missing referral codes during system outages to ensure complete marketing attribution.

### 2. Marketing ROI & Attribution
* **Conversion Analytics**: For self-operated channels, the scripts calculate specific conversion rates (Order/Ticket conversion) for marketing actions like WeChat push articles or Weibo posts.
* **Cross-Platform Consolidation**: Merges weekly performance metrics (followers, views, engagement) from WeChat, Weibo, Douyin, Bilibili, and Xiaohongshu into one unified dashboard.

### 3. Real-Time Inventory Monitoring
* **Cross-System Sync**: A specialized pipeline that reconciles "Direct Sales" inventory with "Venue-held" and "Third-party" inventory.
* **Inventory Visualization**: Automatically generates session-level and price-tier-level reports to show exactly which seats are locked, sold, or available for relocation across different systems.

## 💡 Technical Highlights (Internship Learnings)
* **Regex & String Manipulation**: Used extensive Regular Expressions to handle "messy" real-world data where the same show might be named differently across platforms.
* **Functional Programming**: Implemented factory functions to handle dozens of similar distributor channels efficiently without code bloat.
* **Business-Driven Tech**: Learned how to translate complex theater promotion policies (Early bird tiers, bulk order discounts) into clean, maintainable code.

## 📂 File Structure
* `daily_report_functions.R`: Core utility library for mapping rules, production identifiers, and revenue multipliers.
* `ma_etl.R`: ETL pipeline for *Matilda the Musical*, focused on standardized wide-table generation.
* `ntn_daily_report.qmd`: A specialized reporting tool for *Next to Normal* tour, integrating real-time venue-side inventory and marketing conversion analysis.
* `market_analysis.qmd`: Comprehensive weekly report for cross-platform marketing performance.

## 🔒 Data Privacy & Security
**Important Note:** To comply with data privacy standards and protect corporate confidentiality:
* **Anonymization**: All personally identifiable information (PII) such as customer names, phone numbers, and addresses has been removed or replaced with synthetic data.
* **Redaction**: Sensitive business information, including specific contract values and private client lists, has been de-identified or generalized.
* **Code Only**: This repository is intended to demonstrate data engineering logic and script architecture. No raw, sensitive datasets from the original organization are included in this repository.
---
*Note: These tools were built during my internship to support data-driven ticketing and marketing decisions.*
