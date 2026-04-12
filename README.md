# Ticketing Data Cleaning & Marketing Report Scripts

## 📝 Project Description
This repository contains a set of R scripts I developed during my internship to automate the tedious process of ticketing data reconciliation and marketing performance tracking. 

In the musical theater industry, sales data comes from many different sources (like Damai, Maoyan, and various local distributors), each with its own messy format. My goal was to replace manual Excel copy-pasting with a more reliable **automated data pipeline**.

## 🛠 What These Scripts Do

### 1. Cleaning Messy Data (ETL)
* **Format Standardization**: I wrote scripts to read 30+ different types of Excel reports from various ticketing platforms and turn them into one clean, consistent table.
* **Fixing "Broken" Data**: I designed functions to patch missing referral codes caused by system glitches and to fix inconsistent date formats (like "11.18" vs "2023-11-18").
* **Smart Parsing**: Since many distributors list sales as text (e.g., "Package of 3 tickets"), the script uses regular expressions to automatically extract the actual price and quantity.

### 2. Marketing Attribution
* **Tracking Sales Sources**: The scripts match sales records with specific marketing activities (like a WeChat article or a Weibo post) based on referral codes to see which promotion actually worked.
* **Social Media Summary**: I combined data from various platforms (WeChat, Weibo, Bilibili, Douyin, XHS) into one weekly report to track follower growth and engagement in one place.

### 3. Inventory Monitoring
* **Sales vs. Capacity**: The scripts help track how many tickets have been sold against the theater's actual seat capacity for each session and price tier.

## 💡 Technical Highlights (What I Learned)
* **Handling Real-World Data**: Learned that real data is never "clean." I used a lot of `regex` and `stringr` to handle unexpected text formats.
* **Functional Thinking**: Instead of writing the same code over and over, I started using **factory functions** to handle similar sales channels more efficiently.
* **Pipeline Automation**: Used R's `tidyverse` to build a workflow that takes raw files as input and outputs a final report, reducing the time spent on manual work from hours to minutes.

## 📂 File Structure
* `daily_report_functions.R`: The core "toolbox" containing all mapping rules and cleaning functions.
* `ma_etl.R`: The main script to process raw ticketing files into a master dataset.
* `market_analysis.qmd`: A Quarto document that generates the final marketing performance report.

---
*Note: This project was completed during my internship to support data-driven decision making for theater productions.*
