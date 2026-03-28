🚀 **Real-Time Fraud Detection & Analytics System**
📌 Overview

This project simulates a real-time fraud detection system using Python, MySQL, and Power BI. It generates large-scale transaction data, applies both rule-based and machine learning models, and visualizes fraud insights through an interactive dashboard.

🎯 Problem Statement

Financial systems need to detect fraudulent transactions quickly while maintaining a low false positive rate. This project demonstrates how data pipelines, analytics, and machine learning can be combined to identify suspicious activity.

⚙️ Features
🔄 Real-time transaction simulation
🧠 Fraud detection using:
Rule-based logic
Machine Learning (Logistic Regression)
📊 Bulk data processing (10,000+ records)
🗄️ Database integration using MySQL
📈 Interactive Power BI dashboard
🕒 Time-distributed data for realistic trend analysis

🧠 Tech Stack
--Python (Pandas, Scikit-learn)
--MySQL
--Power BI

🏗️ System Architecture
-Data Generation (Python)
-Data Processing & Fraud Detection
-Storage in MySQL
-Visualization in Power BI

📊 Dashboard Insights
~Total transactions and fraud rate
~Fraud trends over time
~High-risk users
~Location-based fraud analysis



🔍 Key Learnings
-Handling imbalanced datasets (fraud is rare)
-Designing realistic data simulations
-Building end-to-end data pipelines
-Comparing ML models with rule-based systems

🚀 How to Run
1. Install dependencies
    pip install -r requirements.txt
2. Setup MySQL database
    CREATE DATABASE fraud_db;
3. Run the script
    python generator.py
