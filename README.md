# Healthcare Master Data Management (MDM) Demo App

A  Streamlit application for  healthcare Master Data Management operations, powered by Databricks App and ai_query().

Demo walkthrough: https://www.youtube.com/watch?v=g8qPscGfLR4

## 🏥 Overview

This application provides a complete demonstration for managing patient identity resolution and data quality in  healthcare systems. It leverages Databricks SQL Warehouse and Gen AI models to deliver:

- **Patient Record Deduplication**: AI-powered duplicate detection and matching
- **Data Quality Assessment**: Automated quality scoring and completeness analysis
- **Golden Record Management**: Steward-approved master patient records

## ✨ Features

### 📊 Dashboard & Analytics
- Overview of patient data statistics
- Data quality metrics and visualisations
- Source system distribution analysis
- Interactive charts 

### 👥 Patient Records Management
- Patient record viewing
- Search and filtering capabilities

### 🔄 Duplicate Detection & Resolution
- AI-powered similarity analysis using ai_query() and Large Language Models on Databricks
- Confidence scoring for potential matches
- Pairwise record comparison

### 📈 Data Quality Assessment
- Quality scoring
- Completeness analysis across all fields
- Issue identification and categorisation
- Quality trends by source system

### 👨💼 Data Stewardship Interface
- Enhanced pairwise record comparison
- Side-by-side field-level review
- Manual override capabilities
- Approval workflow with comments and audit trail stored in a delta table
- Golden record creation and management

### ⚙️ Configuration Management
- Parameterised database configuration (catalog, schema, tables)
- AI model selection and settings
- Connection testing and validation

