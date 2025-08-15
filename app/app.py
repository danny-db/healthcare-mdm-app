"""
Australian Healthcare Master Data Management (MDM) Streamlit App
===============================================================

A comprehensive Streamlit application for Australian healthcare MDM operations including:
- Patient record deduplication and matching
- Data quality assessment and monitoring
- Golden record creation and management

Designed to run as a Databricks App with SQL Warehouse backend.
"""

import os
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import json
import asyncio
import threading
import time
from functools import wraps

# Ensure environment variable is set correctly
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

# Databricks config
cfg = Config()

# Page configuration
st.set_page_config(
    page_title="Healthcare MDM Dashboard",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state for database configuration
if 'db_config' not in st.session_state:
    st.session_state.db_config = {
        'catalog_name': 'danny_catalog',
        'schema_name': 'healthcare_mdm_schema',
        'table_name': 'patients',
        'golden_table_name': 'patients_gold'
    }

# Initialize session state for AI model configuration
if 'ai_config' not in st.session_state:
    st.session_state.ai_config = {
        'model_name': 'databricks-meta-llama-3-3-70b-instruct'
    }

# Initialize session state for caching and async loading
if 'data_cache' not in st.session_state:
    st.session_state.data_cache = {}
if 'cache_timestamps' not in st.session_state:
    st.session_state.cache_timestamps = {}
if 'loading_states' not in st.session_state:
    st.session_state.loading_states = {}

# Cache expiry time in seconds (5 minutes)
CACHE_EXPIRY = 300

# Custom CSS for healthcare theme
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #e3f2fd 0%, #f8f9fa 50%, #e8f5e8 100%);
        border: 2px solid #4caf50;
        padding: 2rem;
        border-radius: 15px;
        color: #2e7d32;
        text-align: center;
        margin-bottom: 2rem;
        box-shadow: 0 4px 12px rgba(76, 175, 80, 0.15);
    }
    .main-header h1 {
        color: #1b5e20;
        margin-bottom: 0.5rem;
        font-size: 2.2rem;
    }
    .main-header p {
        color: #388e3c;
        font-size: 1.1rem;
        margin-bottom: 1rem;
    }
    .header-icons {
        display: flex;
        justify-content: center;
        gap: 1rem;
        margin-top: 1rem;
    }
    .header-icons span {
        font-size: 1.5rem;
        width: 3rem;
        height: 3rem;
        display: flex;
        align-items: center;
        justify-content: center;
        background: rgba(76, 175, 80, 0.1);
        border-radius: 50%;
        border: 1px solid #4caf50;
    }
    .metric-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border-left: 4px solid #4caf50;
    }
    .quality-high { color: #2e7d32; font-weight: bold; }
    .quality-medium { color: #f57c00; font-weight: bold; }
    .quality-low { color: #d32f2f; font-weight: bold; }
    .sidebar .sidebar-content {
        background-color: #f8f9fa;
    }
    /* Healthcare-themed colors for buttons and elements */
    .stButton > button {
        border-color: #4caf50;
    }
    .stButton > button:hover {
        background-color: #e8f5e8;
        border-color: #388e3c;
    }
</style>
""", unsafe_allow_html=True)

# Query the SQL warehouse with Service Principal credentials
def sql_query_with_service_principal(query: str) -> pd.DataFrame:
    """Execute a SQL query and return the result as a pandas DataFrame."""
    try:
        with sql.connect(
            server_hostname=cfg.host,
            http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
            credentials_provider=lambda: cfg.authenticate
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall_arrow().to_pandas()
    except Exception as e:
        st.error(f"Database connection error: {str(e)}")
        return generate_demo_data()

# Query the SQL warehouse with the user credentials
def sql_query_with_user_token(query: str, user_token: str) -> pd.DataFrame:
    """Execute a SQL query and return the result as a pandas DataFrame."""
    try:
        with sql.connect(
            server_hostname=cfg.host,
            http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
            access_token=user_token
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall_arrow().to_pandas()
    except Exception as e:
        st.error(f"Database connection error: {str(e)}")
        return generate_demo_data()

def generate_demo_data():
    """Generate demo Australian patient data for testing"""
    return pd.DataFrame({
        'patient_id': range(1, 17),
        'medical_record_num': [f'MRN{str(i).zfill(6)}' for i in range(1, 17)],
        'patient_name': ['John Smith', 'J. Smith', 'Jane Doe', 'Jon Smythe', 'Janet Doe', 
                        'John Smith', 'Jake Johnson', 'Janie Doe', 'William Turner', 
                        'Linda Carter', 'Pedro Alvarez', 'Amy Nguyen', 'Marcus Lee',
                        'Sara Patel', 'David Kim', 'Elena Garcia'],
        'medicare_number': ['2428 9123 4567 8', '2428912345678', '2987 6543 2109 8', '2428 9123 4567 8',
                           '2987654321098', None, '2456 7890 1234 5', '2987 6543 2109 8'] * 2,
        'suburb': ['Melbourne', 'Melbourne', 'Carlton', 'Melbourne', 'Carlton', 'Melbourne',
                  'Brisbane', 'Carlton', 'Adelaide', 'Sydney', 'Perth', 'Melbourne',
                  'South Yarra', 'Adelaide', 'Perth', 'Sydney'],
        'state': ['VIC', 'VIC', 'VIC', 'VIC', 'VIC', 'VIC', 'QLD', 'VIC',
                 'SA', 'NSW', 'WA', 'VIC', 'VIC', 'SA', 'WA', 'NSW'],
        'postcode': ['3000', '3000', '3053', '3000', '3053', '3000', '4000', '3053',
                    '5000', '2000', '6000', '3000', '3141', '5000', '6000', '2000'],
        'source_system': ['EMR_System_A', 'EMR_System_B', 'EMR_System_A', 'Lab_System',
                         'Billing_System', 'Registration_System', 'EMR_System_A', 
                         'Pharmacy_System'] * 2,
        'private_health_fund': ['Medibank', 'Medibank Private', 'BUPA', 'Medibank',
                               'BUPA Health', 'Medibank', 'HCF', 'BUPA'] * 2,
        'blood_type': ['O+', 'O+', 'A-', 'O+', 'A-', None, 'B+', 'A-'] * 2,
        'gender': ['M', 'Male', 'F', 'M', 'Female', 'M', 'M', 'F'] * 2
    })

def generate_demo_quality_data():
    """Generate demo quality assessment data"""
    return pd.DataFrame({
        'patient_id': range(1, 17),
        'patient_name': ['John Smith', 'J. Smith', 'Jane Doe', 'Jon Smythe', 'Janet Doe', 
                        'John Smith', 'Jake Johnson', 'Janie Doe', 'William Turner', 
                        'Linda Carter', 'Pedro Alvarez', 'Amy Nguyen', 'Marcus Lee',
                        'Sara Patel', 'David Kim', 'Elena Garcia'],
        'source_system': ['EMR_System_A', 'EMR_System_B', 'EMR_System_A', 'Lab_System',
                         'Billing_System', 'Registration_System', 'EMR_System_A', 
                         'Pharmacy_System'] * 2,
        'quality_score': [95, 88, 92, 85, 78, 65, 90, 87, 93, 89, 82, 94, 86, 91, 83, 88],
        'completeness': [0.95, 0.82, 0.89, 0.78, 0.71, 0.58, 0.87, 0.83, 0.91, 0.85, 0.79, 0.92, 0.81, 0.88, 0.80, 0.84],
        'issues': [['None'], ['Missing emergency contact'], ['None'], ['Missing blood type'], 
                  ['Missing Medicare number', 'Incomplete address'], ['Missing Medicare number', 'Missing phone', 'Missing blood type'],
                  ['None'], ['Missing emergency contact'], ['None'], ['None'], ['Missing blood type'],
                  ['None'], ['Missing emergency contact'], ['None'], ['Missing blood type'], ['None']]
    })

def is_cache_valid(cache_key):
    """Check if cached data is still valid"""
    if cache_key not in st.session_state.cache_timestamps:
        return False
    
    cache_time = st.session_state.cache_timestamps[cache_key]
    return (time.time() - cache_time) < CACHE_EXPIRY

def get_cached_data(cache_key):
    """Get data from cache if valid"""
    if is_cache_valid(cache_key) and cache_key in st.session_state.data_cache:
        return st.session_state.data_cache[cache_key]
    return None

def set_cached_data(cache_key, data):
    """Store data in cache with timestamp"""
    st.session_state.data_cache[cache_key] = data
    st.session_state.cache_timestamps[cache_key] = time.time()

def clear_cache():
    """Clear all cached data"""
    st.session_state.data_cache = {}
    st.session_state.cache_timestamps = {}

def async_query_wrapper(func):
    """Decorator to add async loading capabilities to query functions"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Generate cache key based on function name and table config
        config = st.session_state.db_config
        cache_key = f"{func.__name__}_{config['catalog_name']}_{config['schema_name']}_{config['table_name']}"
        
        # Check cache first
        cached_data = get_cached_data(cache_key)
        if cached_data is not None:
            return cached_data
        
        # If not in cache, execute query
        try:
            result = func(*args, **kwargs)
            set_cached_data(cache_key, result)
            return result
        except Exception as e:
            st.error(f"Query failed: {str(e)}")
            # Return demo data as fallback
            if 'patient' in func.__name__:
                return generate_demo_data()
            elif 'quality' in func.__name__:
                return generate_demo_quality_data()
            else:
                return pd.DataFrame()
    
    return wrapper

def show_loading_spinner(message="Loading data..."):
    """Show a loading spinner with message"""
    return st.spinner(message)

def create_progress_placeholder():
    """Create a placeholder for progress updates"""
    return st.empty()

def get_ai_model_name():
    """Get the configured AI model name"""
    return st.session_state.ai_config['model_name']

def get_table_reference(table_type='main'):
    """Get fully qualified table reference"""
    config = st.session_state.db_config
    if table_type == 'golden':
        return f"{config['catalog_name']}.{config['schema_name']}.{config['golden_table_name']}"
    else:
        return f"{config['catalog_name']}.{config['schema_name']}.{config['table_name']}"

def create_golden_table_if_not_exists(user_token=None):
    """Create the golden records table if it doesn't exist"""
    table_ref = get_table_reference('golden')
    
    # Try the modern approach first (with Delta features)
    create_query_modern = f"""
        CREATE TABLE IF NOT EXISTS {table_ref} (
            golden_record_id BIGINT GENERATED ALWAYS AS IDENTITY,
            patient_id_cluster STRING,
            medical_record_num STRING,
            patient_name STRING,
            date_of_birth DATE,
            medicare_number STRING,
            phone STRING,
            email STRING,
            address STRING,
            suburb STRING,
            state STRING,
            postcode STRING,
            private_health_fund STRING,
            membership_number STRING,
            emergency_contact STRING,
            gp_name STRING,
            blood_type STRING,
            gender STRING,
            confidence_score DOUBLE,
            source_patient_ids STRING,
            steward_status STRING,
            steward_comments STRING,
            approved_by STRING,
            approved_at TIMESTAMP,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        ) USING DELTA
        TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
    """
    
    # Fallback approach (without Delta features)
    create_query_fallback = f"""
        CREATE TABLE IF NOT EXISTS {table_ref} (
            golden_record_id BIGINT GENERATED ALWAYS AS IDENTITY,
            patient_id_cluster STRING,
            medical_record_num STRING,
            patient_name STRING,
            date_of_birth DATE,
            medicare_number STRING,
            phone STRING,
            email STRING,
            address STRING,
            suburb STRING,
            state STRING,
            postcode STRING,
            private_health_fund STRING,
            membership_number STRING,
            emergency_contact STRING,
            gp_name STRING,
            blood_type STRING,
            gender STRING,
            confidence_score DOUBLE,
            source_patient_ids STRING,
            steward_status STRING,
            steward_comments STRING,
            approved_by STRING,
            approved_at TIMESTAMP,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        ) USING DELTA
    """
    
    try:
        # Try modern approach first
        try:
            if user_token:
                sql_query_with_user_token(create_query_modern, user_token)
            else:
                sql_query_with_service_principal(create_query_modern)
            st.success("✅ Golden records table created with Delta features enabled")
        except Exception as modern_e:
            if "WRONG_COLUMN_DEFAULTS_FOR_DELTA_FEATURE_NOT_ENABLED" in str(modern_e):
                # Fall back to basic table creation
                st.info("ℹ️ Using fallback table creation (Delta column defaults not supported)")
                if user_token:
                    sql_query_with_user_token(create_query_fallback, user_token)
                else:
                    sql_query_with_service_principal(create_query_fallback)
                st.success("✅ Golden records table created (basic mode)")
            else:
                raise modern_e
        
        return True
    except Exception as e:
        st.error(f"❌ Error creating golden table: {str(e)}")
        return False

@async_query_wrapper
def fetch_patient_data(user_token=None):
    """Fetch patient data from Databricks SQL Warehouse"""
    table_ref = get_table_reference('main')
    query = f"""
        SELECT patient_id, medical_record_num, patient_name, date_of_birth, 
               medicare_number, phone, email, address, suburb, state, postcode,
               private_health_fund, membership_number, emergency_contact,
               gp_name, blood_type, gender, source_system
        FROM {table_ref}
        ORDER BY patient_id
    """
    
    if user_token:
        return sql_query_with_user_token(query, user_token)
    else:
        return sql_query_with_service_principal(query)

@async_query_wrapper
def fetch_quality_data(user_token=None):
    """Fetch quality assessment data from Databricks SQL Warehouse"""
    table_ref = get_table_reference('main')
    model_name = get_ai_model_name()
    query = f"""
        WITH patient_quality AS (
            SELECT
                patient_id, patient_name, source_system,
                ai_query(
                    '{model_name}',
                    CONCAT(
                        'Analyze Australian healthcare record quality. Return quality_score (0-100), completeness (0-1), issues (array of strings). ',
                        'Record: ', to_json(struct(medical_record_num, patient_name, date_of_birth, medicare_number, phone, email, private_health_fund))
                    ),
                    responseFormat => '{{"type": "json_schema", "json_schema": {{"name": "quality", "schema": {{"type": "object", "properties": {{"quality_score": {{"type": "integer"}}, "completeness": {{"type": "number"}}, "issues": {{"type": "array", "items": {{"type": "string"}}}}}}, "required": ["quality_score", "completeness", "issues"]}}}}}}'
                ) AS quality_assessment
            FROM {table_ref}
        )
        SELECT patient_id, patient_name, source_system,
               get_json_object(quality_assessment, '$.quality_score') AS quality_score,
               get_json_object(quality_assessment, '$.completeness') AS completeness,
               get_json_object(quality_assessment, '$.issues') AS issues
        FROM patient_quality
        ORDER BY CAST(get_json_object(quality_assessment, '$.quality_score') AS INT) DESC
    """
    
    if user_token:
        return sql_query_with_user_token(query, user_token)
    else:
        return sql_query_with_service_principal(query)

@async_query_wrapper
def fetch_duplicate_data(user_token=None):
    """Fetch duplicate detection results from Databricks SQL Warehouse"""
    table_ref = get_table_reference('main')
    model_name = get_ai_model_name()
    query = f"""
        WITH pairwise_similarity AS (
            SELECT 
                t1.patient_id AS id1, t2.patient_id AS id2,
                t1.patient_name AS name1, t2.patient_name AS name2,
                t1.source_system AS system1, t2.source_system AS system2,
                ai_query(
                    '{model_name}',
                    CONCAT(
                        'Compare these two Australian patient records and determine if they represent the same person. ',
                        'Return similarity_score (0-1), is_match (boolean), confidence (low/medium/high), match_reason. ',
                        'Patient 1: {{name: ', t1.patient_name, ', dob: ', t1.date_of_birth, ', medicare: ', t1.medicare_number, '}}. ',
                        'Patient 2: {{name: ', t2.patient_name, ', dob: ', t2.date_of_birth, ', medicare: ', t2.medicare_number, '}}.'
                    ),
                    responseFormat => '{{"type": "json_schema", "json_schema": {{"name": "similarity", "schema": {{"type": "object", "properties": {{"similarity_score": {{"type": "number"}}, "is_match": {{"type": "boolean"}}, "confidence": {{"type": "string"}}, "match_reason": {{"type": "string"}}}}, "required": ["similarity_score", "is_match", "confidence", "match_reason"]}}}}}}'
                ) AS similarity_result
            FROM {table_ref} t1
            CROSS JOIN {table_ref} t2 
            WHERE t1.patient_id < t2.patient_id
        )
        SELECT id1, id2, name1, name2, system1, system2,
               get_json_object(similarity_result, '$.similarity_score') AS similarity_score,
               get_json_object(similarity_result, '$.is_match') AS is_match,
               get_json_object(similarity_result, '$.confidence') AS confidence,
               get_json_object(similarity_result, '$.match_reason') AS match_reason
        FROM pairwise_similarity
        WHERE get_json_object(similarity_result, '$.similarity_score') > 0.5
        ORDER BY get_json_object(similarity_result, '$.similarity_score') DESC
    """
    
    if user_token:
        return sql_query_with_user_token(query, user_token)
    else:
        return sql_query_with_service_principal(query)

@async_query_wrapper
def fetch_golden_records(user_token=None):
    """Fetch golden records for stewardship review"""
    table_ref = get_table_reference('golden')
    query = f"""
        SELECT golden_record_id, patient_id_cluster, medical_record_num, patient_name,
               date_of_birth, medicare_number, phone, email, address, suburb, state, postcode,
               private_health_fund, membership_number, emergency_contact, gp_name, blood_type, gender,
               confidence_score, source_patient_ids, steward_status, steward_comments,
               approved_by, approved_at, created_at, updated_at
        FROM {table_ref}
        ORDER BY created_at DESC
    """
    
    try:
        if user_token:
            return sql_query_with_user_token(query, user_token)
        else:
            return sql_query_with_service_principal(query)
    except Exception:
        # Return empty DataFrame if table doesn't exist
        return pd.DataFrame()

def generate_golden_records(user_token=None):
    """Generate golden records from duplicate detection results"""
    table_ref_main = get_table_reference('main')
    table_ref_golden = get_table_reference('golden')
    model_name = get_ai_model_name()
    
    query = f"""
        WITH high_confidence_matches AS (
            SELECT 
                t1.patient_id AS id1, t2.patient_id AS id2,
                t1.medical_record_num AS mrn1, t2.medical_record_num AS mrn2,
                t1.patient_name AS name1, t2.patient_name AS name2,
                t1.date_of_birth AS dob1, t2.date_of_birth AS dob2,
                t1.medicare_number AS medicare1, t2.medicare_number AS medicare2,
                t1.phone AS phone1, t2.phone AS phone2,
                t1.email AS email1, t2.email AS email2,
                t1.address AS address1, t2.address AS address2,
                t1.suburb AS suburb1, t2.suburb AS suburb2,
                t1.state AS state1, t2.state AS state2,
                t1.postcode AS postcode1, t2.postcode AS postcode2,
                t1.private_health_fund AS fund1, t2.private_health_fund AS fund2,
                t1.membership_number AS member1, t2.membership_number AS member2,
                t1.emergency_contact AS emergency1, t2.emergency_contact AS emergency2,
                t1.gp_name AS gp1, t2.gp_name AS gp2,
                t1.blood_type AS blood1, t2.blood_type AS blood2,
                t1.gender AS gender1, t2.gender AS gender2,
                ai_query(
                    '{model_name}',
                    CONCAT(
                        'Create the best golden record from these two Australian patient records. ',
                        'Choose the most complete and accurate values. Return as JSON with all patient fields and confidence (0-1). ',
                        'Patient 1: {{', 
                        'mrn: ', t1.medical_record_num, ', name: ', t1.patient_name, ', dob: ', t1.date_of_birth, 
                        ', medicare: ', t1.medicare_number, ', phone: ', t1.phone, ', email: ', t1.email,
                        ', address: ', t1.address, ', suburb: ', t1.suburb, ', state: ', t1.state, ', postcode: ', t1.postcode,
                        ', fund: ', t1.private_health_fund, ', member: ', t1.membership_number, ', emergency: ', t1.emergency_contact,
                        ', gp: ', t1.gp_name, ', blood: ', t1.blood_type, ', gender: ', t1.gender, '}}. ',
                        'Patient 2: {{',
                        'mrn: ', t2.medical_record_num, ', name: ', t2.patient_name, ', dob: ', t2.date_of_birth,
                        ', medicare: ', t2.medicare_number, ', phone: ', t2.phone, ', email: ', t2.email,
                        ', address: ', t2.address, ', suburb: ', t2.suburb, ', state: ', t2.state, ', postcode: ', t2.postcode,
                        ', fund: ', t2.private_health_fund, ', member: ', t2.membership_number, ', emergency: ', t2.emergency_contact,
                        ', gp: ', t2.gp_name, ', blood: ', t2.blood_type, ', gender: ', t2.gender, '}}'
                    ),
                    responseFormat => '{{"type": "json_schema", "json_schema": {{"name": "golden_record", "schema": {{"type": "object", "properties": {{"medical_record_num": {{"type": "string"}}, "patient_name": {{"type": "string"}}, "date_of_birth": {{"type": "string"}}, "medicare_number": {{"type": "string"}}, "phone": {{"type": "string"}}, "email": {{"type": "string"}}, "address": {{"type": "string"}}, "suburb": {{"type": "string"}}, "state": {{"type": "string"}}, "postcode": {{"type": "string"}}, "private_health_fund": {{"type": "string"}}, "membership_number": {{"type": "string"}}, "emergency_contact": {{"type": "string"}}, "gp_name": {{"type": "string"}}, "blood_type": {{"type": "string"}}, "gender": {{"type": "string"}}, "confidence": {{"type": "number"}}}}, "required": ["medical_record_num", "patient_name", "date_of_birth", "medicare_number", "phone", "email", "address", "suburb", "state", "postcode", "private_health_fund", "membership_number", "emergency_contact", "gp_name", "blood_type", "gender", "confidence"]}}}}}}'
                ) AS golden_result,
                CONCAT(t1.patient_id, ',', t2.patient_id) AS source_ids
            FROM {table_ref_main} t1
            CROSS JOIN {table_ref_main} t2 
            WHERE t1.patient_id < t2.patient_id
            AND ai_query(
                '{model_name}',
                CONCAT('Are these the same person? Return only true/false. Patient 1: ', t1.patient_name, ' ', t1.date_of_birth, ' ', t1.medicare_number, '. Patient 2: ', t2.patient_name, ' ', t2.date_of_birth, ' ', t2.medicare_number)
            ) = 'true'
        )
        INSERT INTO {table_ref_golden} (
            patient_id_cluster, medical_record_num, patient_name, date_of_birth,
            medicare_number, phone, email, address, suburb, state, postcode,
            private_health_fund, membership_number, emergency_contact, gp_name,
            blood_type, gender, confidence_score, source_patient_ids,
            steward_status, created_at, updated_at
        )
        SELECT 
            source_ids,
            get_json_object(golden_result, '$.medical_record_num'),
            get_json_object(golden_result, '$.patient_name'),
            get_json_object(golden_result, '$.date_of_birth'),
            get_json_object(golden_result, '$.medicare_number'),
            get_json_object(golden_result, '$.phone'),
            get_json_object(golden_result, '$.email'),
            get_json_object(golden_result, '$.address'),
            get_json_object(golden_result, '$.suburb'),
            get_json_object(golden_result, '$.state'),
            get_json_object(golden_result, '$.postcode'),
            get_json_object(golden_result, '$.private_health_fund'),
            get_json_object(golden_result, '$.membership_number'),
            get_json_object(golden_result, '$.emergency_contact'),
            get_json_object(golden_result, '$.gp_name'),
            get_json_object(golden_result, '$.blood_type'),
            get_json_object(golden_result, '$.gender'),
            CAST(get_json_object(golden_result, '$.confidence') AS DOUBLE),
            source_ids,
            'pending',
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
        FROM high_confidence_matches
    """
    
    try:
        if user_token:
            sql_query_with_user_token(query, user_token)
        else:
            sql_query_with_service_principal(query)
        return True
    except Exception as e:
        st.error(f"Error generating golden records: {str(e)}")
        return False

def update_golden_record_status(record_id, status, comments, approved_by, user_token=None):
    """Update the stewardship status of a golden record"""
    table_ref = get_table_reference('golden')
    query = f"""
        UPDATE {table_ref}
        SET steward_status = '{status}',
            steward_comments = '{comments}',
            approved_by = '{approved_by}',
            approved_at = CURRENT_TIMESTAMP(),
            updated_at = CURRENT_TIMESTAMP()
        WHERE golden_record_id = {record_id}
    """
    
    try:
        if user_token:
            sql_query_with_user_token(query, user_token)
        else:
            sql_query_with_service_principal(query)
        return True
    except Exception as e:
        st.error(f"Error updating record status: {str(e)}")
        return False

def update_golden_record_with_steward_edits(record_id, updated_data, status, comments, approved_by, user_token=None):
    """Update golden record with steward's edited data"""
    table_ref = get_table_reference('golden')
    
    # Build the SET clause dynamically from updated_data
    set_clauses = []
    for field, value in updated_data.items():
        if value is not None:
            escaped_value = str(value).replace("'", "''")  # Escape single quotes
            set_clauses.append(f"{field} = '{escaped_value}'")
        else:
            set_clauses.append(f"{field} = NULL")
    
    set_clause = ", ".join(set_clauses)
    
    query = f"""
        UPDATE {table_ref}
        SET {set_clause},
            steward_status = '{status}',
            steward_comments = '{comments}',
            approved_by = '{approved_by}',
            approved_at = CURRENT_TIMESTAMP(),
            updated_at = CURRENT_TIMESTAMP()
        WHERE golden_record_id = {record_id}
    """
    
    try:
        if user_token:
            sql_query_with_user_token(query, user_token)
        else:
            sql_query_with_service_principal(query)
        return True
    except Exception as e:
        st.error(f"Error updating golden record: {str(e)}")
        return False

def fetch_source_patient_records(source_patient_ids, user_token=None):
    """Fetch the original patient records that contributed to a golden record"""
    table_ref = get_table_reference('main')
    patient_ids = source_patient_ids.split(',')
    ids_str = ','.join(patient_ids)
    
    query = f"""
        SELECT patient_id, medical_record_num, patient_name, date_of_birth, 
               medicare_number, phone, email, address, suburb, state, postcode,
               private_health_fund, membership_number, emergency_contact,
               gp_name, blood_type, gender, source_system
        FROM {table_ref}
        WHERE patient_id IN ({ids_str})
        ORDER BY patient_id
    """
    
    try:
        if user_token:
            return sql_query_with_user_token(query, user_token)
        else:
            return sql_query_with_service_principal(query)
    except Exception as e:
        st.error(f"Error fetching source records: {str(e)}")
        return pd.DataFrame()

def show_overview_dashboard(patient_data, quality_data, duplicate_data):
    """Display the main overview dashboard"""
    st.markdown("""
    <div class="main-header">
        <h1>🏥 Healthcare Master Data Management Dashboard</h1>
        <p>🤖 AI-Powered Patient Identity Resolution & Data Quality Management</p>
        <div class="header-icons">
            <span>🩺</span>
            <span>📋</span>
            <span>🔬</span>
            <span>💊</span>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_patients = len(patient_data)
        st.metric("Total Patients", total_patients, delta=None)
    
    with col2:
        potential_duplicates = len(duplicate_data[duplicate_data['is_match'] == 'true']) if len(duplicate_data) > 0 else 0
        st.metric("Potential Duplicates", potential_duplicates, delta=None)
    
    with col3:
        avg_quality = quality_data['quality_score'].astype(float).mean()
        st.metric("Avg Data Quality", f"{avg_quality:.1f}/100", delta=None)
    
    with col4:
        avg_completeness = quality_data['completeness'].astype(float).mean() * 100
        st.metric("Avg Completeness", f"{avg_completeness:.1f}%", delta=None)
    
    # Charts row
    col1, col2 = st.columns(2)
    
    with col1:
        # Source system distribution
        system_counts = patient_data['source_system'].value_counts()
        fig_systems = px.pie(
            values=system_counts.values,
            names=system_counts.index,
            title="Patient Records by Source System"
        )
        st.plotly_chart(fig_systems, use_container_width=True)
    
    with col2:
        # Quality score distribution
        fig_quality = px.histogram(
            quality_data,
            x='quality_score',
            nbins=20,
            title="Data Quality Score Distribution",
            labels={'quality_score': 'Quality Score', 'count': 'Number of Records'}
        )
        st.plotly_chart(fig_quality, use_container_width=True)

def show_patient_records(patient_data):
    """Display patient records with filtering"""
    st.header("👥 Patient Records Management")
    
    # Search and filter options
    col1, col2, col3 = st.columns(3)
    
    with col1:
        search_term = st.text_input("🔍 Search patients", placeholder="Enter name, MRN, or Medicare number")
    
    with col2:
        source_filter = st.selectbox("Filter by Source System", 
                                   ["All Systems"] + list(patient_data['source_system'].unique()))
    
    with col3:
        health_fund_filter = st.selectbox("Filter by Health Fund", 
                                        ["All Providers"] + list(patient_data['private_health_fund'].unique()))
    
    # Apply filters
    filtered_data = patient_data.copy()
    
    if search_term:
        filtered_data = filtered_data[
            filtered_data['patient_name'].str.contains(search_term, case=False, na=False) |
            filtered_data['medical_record_num'].str.contains(search_term, case=False, na=False) |
            filtered_data['medicare_number'].str.contains(search_term, case=False, na=False)
        ]
    
    if source_filter != "All Systems":
        filtered_data = filtered_data[filtered_data['source_system'] == source_filter]
    
    if health_fund_filter != "All Providers":
        filtered_data = filtered_data[filtered_data['private_health_fund'] == health_fund_filter]
    
    # Display results
    st.subheader(f"📋 Patient Records ({len(filtered_data)} records)")
    
    if not filtered_data.empty:
        # Select key columns for display
        display_columns = ['patient_id', 'medical_record_num', 'patient_name', 
                          'source_system', 'private_health_fund', 'blood_type', 'gender']
        
        st.dataframe(
            filtered_data[display_columns],
            use_container_width=True,
            column_config={
                "patient_id": st.column_config.NumberColumn("Patient ID", format="%d"),
                "medical_record_num": st.column_config.TextColumn("MRN"),
                "patient_name": st.column_config.TextColumn("Patient Name"),
                "source_system": st.column_config.TextColumn("Source System"),
                "private_health_fund": st.column_config.TextColumn("Health Fund"),
                "blood_type": st.column_config.TextColumn("Blood Type"),
                "gender": st.column_config.TextColumn("Gender")
            }
        )
    else:
        st.info("No records found matching your criteria.")

def show_data_quality(quality_data):
    """Display data quality assessment"""
    st.header("📊 Data Quality Assessment")
    
    # Quality score distribution
    col1, col2 = st.columns(2)
    
    with col1:
        fig_quality_dist = px.histogram(
            quality_data,
            x='quality_score',
            nbins=15,
            title="Quality Score Distribution",
            labels={'quality_score': 'Quality Score', 'count': 'Number of Records'}
        )
        st.plotly_chart(fig_quality_dist, use_container_width=True)
    
    with col2:
        # Quality by source system
        fig_box = px.box(
            quality_data,
            x='source_system',
            y='quality_score',
            title="Quality Score by Source System"
        )
        fig_box.update_xaxes(tickangle=45)
        st.plotly_chart(fig_box, use_container_width=True)
    
    # Data Completeness Analysis
    st.subheader("📊 Data Completeness Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Completeness distribution
        fig_completeness = px.histogram(
            quality_data,
            x='completeness',
            nbins=10,
            title="Data Completeness Distribution",
            labels={'completeness': 'Completeness Score', 'count': 'Number of Records'}
        )
        st.plotly_chart(fig_completeness, use_container_width=True)
    
    with col2:
        # Quality score vs completeness scatter
        fig_scatter = px.scatter(
            quality_data,
            x='completeness',
            y='quality_score',
            color='source_system',
            title="Quality Score vs Completeness by Source System",
            labels={'completeness': 'Completeness Score', 'quality_score': 'Quality Score'}
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
    
    # Detailed quality table
    st.subheader("📋 Detailed Quality Assessment")
    st.dataframe(quality_data, use_container_width=True)

def show_duplicate_detection(duplicate_data):
    """Display duplicate detection results"""
    st.header("🔄 Duplicate Detection & Resolution")
    
    if len(duplicate_data) > 0:
        # Summary metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total_pairs = len(duplicate_data)
            st.metric("Total Comparisons", total_pairs)
        
        with col2:
            likely_matches = len(duplicate_data[duplicate_data['is_match'] == 'true'])
            st.metric("Likely Matches", likely_matches)
        
        with col3:
            avg_similarity = duplicate_data['similarity_score'].astype(float).mean()
            st.metric("Avg Similarity", f"{avg_similarity:.3f}")
        
        # Similarity distribution
        fig_similarity = px.histogram(
            duplicate_data,
            x='similarity_score',
            nbins=20,
            title="Similarity Score Distribution",
            labels={'similarity_score': 'Similarity Score', 'count': 'Number of Pairs'}
        )
        st.plotly_chart(fig_similarity, use_container_width=True)
        
        # Detailed results
        st.subheader("🔍 Detailed Duplicate Analysis")
        
        # Filter for high similarity pairs
        high_similarity = duplicate_data[duplicate_data['similarity_score'].astype(float) > 0.7]
        
        if len(high_similarity) > 0:
            st.dataframe(high_similarity, use_container_width=True)
        else:
            st.info("No high-similarity pairs found.")
    else:
        st.info("No duplicate analysis data available.")

def show_enhanced_stewardship_interface(record, steward_name, user_token):
    """Show enhanced stewardship interface with pairwise comparison and editable fields"""
    
    # Fetch source patient records with loading indicator
    with st.spinner("Loading source patient records for comparison..."):
        source_records = fetch_source_patient_records(record['source_patient_ids'], user_token)
    
    if source_records.empty:
        st.error("Could not fetch source patient records for comparison.")
        return
    
    st.markdown("### 🔍 Pairwise Record Comparison & Golden Record Creation")
    
    # Define the fields we want to compare and edit
    field_mapping = {
        'medical_record_num': 'Medical Record Number',
        'patient_name': 'Patient Name',
        'date_of_birth': 'Date of Birth',
        'medicare_number': 'Medicare Number',
        'phone': 'Phone',
        'email': 'Email',
        'address': 'Address',
        'suburb': 'Suburb',
        'state': 'State',
        'postcode': 'Postcode',
        'private_health_fund': 'Private Health Fund',
        'membership_number': 'Membership Number',
        'emergency_contact': 'Emergency Contact',
        'gp_name': 'GP Name',
        'blood_type': 'Blood Type',
        'gender': 'Gender'
    }
    
    # Initialize session state for this record's selections
    record_key = f"record_{record['golden_record_id']}"
    if f"{record_key}_selections" not in st.session_state:
        st.session_state[f"{record_key}_selections"] = {}
        st.session_state[f"{record_key}_edited_values"] = {}
    
    # Show source records side by side
    st.markdown("#### 📊 Source Records Comparison")
    
    if len(source_records) >= 2:
        record_a = source_records.iloc[0]
        record_b = source_records.iloc[1]
        
        # Header for comparison
        col1, col2, col3, col4, col5 = st.columns([2, 2, 2, 1, 2])
        with col1:
            st.markdown("**Field**")
        with col2:
            st.markdown(f"**Record A** (ID: {record_a['patient_id']})")
            st.caption(f"Source: {record_a['source_system']}")
        with col3:
            st.markdown(f"**Record B** (ID: {record_b['patient_id']})")
            st.caption(f"Source: {record_b['source_system']}")
        with col4:
            st.markdown("**Select**")
        with col5:
            st.markdown("**Golden Record**")
        
        st.markdown("---")
        
        # Create comparison interface for each field
        updated_golden_record = {}
        
        # Use columns for better layout
        for field_key, field_name in field_mapping.items():
            col1, col2, col3, col4, col5 = st.columns([2, 2, 2, 1, 2])
            
            value_a = str(record_a[field_key]) if pd.notna(record_a[field_key]) else ""
            value_b = str(record_b[field_key]) if pd.notna(record_b[field_key]) else ""
            current_golden_value = str(record[field_key]) if pd.notna(record[field_key]) else ""
            
            with col1:
                st.markdown(f"**{field_name}**")
            
            with col2:
                # Show Record A value
                if value_a:
                    st.text_area("Record A", value=value_a, height=60, disabled=True, key=f"{record_key}_{field_key}_display_a")
                else:
                    st.text_area("Record A", value="(empty)", height=60, disabled=True, key=f"{record_key}_{field_key}_display_a_empty")
            
            with col3:
                # Show Record B value
                if value_b:
                    st.text_area("Record B", value=value_b, height=60, disabled=True, key=f"{record_key}_{field_key}_display_b")
                else:
                    st.text_area("Record B", value="(empty)", height=60, disabled=True, key=f"{record_key}_{field_key}_display_b_empty")
            
            with col4:
                # Selection radio buttons (no page refresh)
                options = []
                if value_a:
                    options.append("A")
                if value_b:
                    options.append("B")
                options.append("Manual")
                
                # Get current selection
                current_selection = st.session_state.get(f"{record_key}_selections", {}).get(field_key, "Manual")
                if current_selection not in options:
                    current_selection = "Manual"
                
                selection = st.radio(
                    "Choose",
                    options,
                    index=options.index(current_selection) if current_selection in options else len(options)-1,
                    key=f"{record_key}_{field_key}_selection",
                    label_visibility="visible"
                )
                
                # Update session state based on selection
                st.session_state[f"{record_key}_selections"][field_key] = selection
            
            with col5:
                # Determine the value to show in the text input based on selection
                if selection == "A" and value_a:
                    default_value = value_a
                elif selection == "B" and value_b:
                    default_value = value_b
                else:
                    # Manual or no valid selection - use current golden value or previous manual input
                    default_value = st.session_state.get(f"{record_key}_{field_key}_manual", current_golden_value)
                
                # Editable text input for golden record
                edited_value = st.text_area(
                    "Golden Record",
                    value=default_value,
                    height=60,
                    key=f"{record_key}_{field_key}_edit"
                )
                
                # Store manual edits separately
                if selection == "Manual":
                    st.session_state[f"{record_key}_{field_key}_manual"] = edited_value
                
                updated_golden_record[field_key] = edited_value if edited_value else None
        
        st.markdown("---")
        
        # Stewardship decision section
        st.markdown("#### 👨‍💼 Stewardship Decision")
        
        col1, col2 = st.columns([3, 1])
        
        with col1:
            comments = st.text_area(
                "Review Comments",
                placeholder="Add your review comments here...",
                key=f"{record_key}_comments"
            )
        
        with col2:
            st.markdown("**Actions**")
            
            if st.button("✅ Approve Golden Record", key=f"{record_key}_approve", type="primary"):
                with st.spinner("Approving golden record..."):
                    success = update_golden_record_with_steward_edits(
                        record['golden_record_id'],
                        updated_golden_record,
                        'approved',
                        comments or 'Approved with steward edits',
                        steward_name,
                        user_token
                    )
                    
                if success:
                    st.success("✅ Golden record approved with your edits!")
                    # Clear session state for this record
                    if f"{record_key}_selections" in st.session_state:
                        del st.session_state[f"{record_key}_selections"]
                    if f"{record_key}_edited_values" in st.session_state:
                        del st.session_state[f"{record_key}_edited_values"]
                    st.rerun()
                else:
                    st.error("Failed to approve record. Please try again.")
            
            if st.button("❌ Reject Record", key=f"{record_key}_reject", type="secondary"):
                if comments:
                    with st.spinner("Rejecting record..."):
                        success = update_golden_record_status(
                            record['golden_record_id'],
                            'rejected',
                            comments,
                            steward_name,
                            user_token
                        )
                    
                    if success:
                        st.success("❌ Record rejected!")
                        # Clear session state for this record
                        if f"{record_key}_selections" in st.session_state:
                            del st.session_state[f"{record_key}_selections"]
                        if f"{record_key}_edited_values" in st.session_state:
                            del st.session_state[f"{record_key}_edited_values"]
                        st.rerun()
                    else:
                        st.error("Failed to reject record. Please try again.")
                else:
                    st.error("Please provide comments when rejecting a record.")
    
    else:
        st.warning("Not enough source records found for comparison.")

def show_data_stewardship(user_token=None):
    """Display data stewardship page for golden record approval"""
    st.header("👨‍💼 Data Stewardship - Golden Record Review")
    
    # Create golden table if it doesn't exist
    if create_golden_table_if_not_exists(user_token):
        st.success("✅ Golden records table is ready")
    
    # Control panel
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔄 Generate New Golden Records", type="primary"):
            # Create progress indicators
            progress_placeholder = st.empty()
            status_placeholder = st.empty()
            
            with progress_placeholder.container():
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                status_text.text("🔍 Analyzing duplicate detection results...")
                progress_bar.progress(20)
                
                status_text.text("🤖 Generating golden records with AI...")
                progress_bar.progress(50)
                
                status_text.text("💾 Saving golden records to database...")
                progress_bar.progress(80)
                
                success = generate_golden_records(user_token)
                progress_bar.progress(100)
                
                if success:
                    status_text.text("✅ Golden records generated successfully!")
                    time.sleep(1)  # Brief pause to show completion
                    progress_placeholder.empty()
                    st.success("✅ Golden records generated successfully!")
                    st.rerun()
                else:
                    status_text.text("❌ Failed to generate golden records")
                    time.sleep(1)
                    progress_placeholder.empty()
                    st.error("❌ Failed to generate golden records")
    
    with col2:
        steward_name = st.text_input("👤 Data Steward Name", value="Data Steward", key="steward_name")
    
    with col3:
        status_filter = st.selectbox("Filter by Status", ["All", "pending", "approved", "rejected"])
    
    # Fetch golden records with loading indicator
    with st.spinner("Loading golden records..."):
        golden_records = fetch_golden_records(user_token)
    
    if golden_records.empty:
        st.info("📝 No golden records found. Generate some records first using the button above.")
        return
    
    # Apply status filter
    if status_filter != "All":
        golden_records = golden_records[golden_records['steward_status'] == status_filter]
    
    # Display summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_records = len(golden_records)
        st.metric("Total Golden Records", total_records)
    
    with col2:
        pending_count = len(golden_records[golden_records['steward_status'] == 'pending'])
        st.metric("Pending Review", pending_count)
    
    with col3:
        approved_count = len(golden_records[golden_records['steward_status'] == 'approved'])
        st.metric("Approved", approved_count)
    
    with col4:
        rejected_count = len(golden_records[golden_records['steward_status'] == 'rejected'])
        st.metric("Rejected", rejected_count)
    
    # Display records for review
    st.subheader("📋 Golden Records for Review")
    
    if not golden_records.empty:
        for idx, record in golden_records.iterrows():
            with st.expander(f"🏥 Record #{record['golden_record_id']} - {record['patient_name']} (Confidence: {record['confidence_score']:.2f})", 
                           expanded=(record['steward_status'] == 'pending')):
                
                # Show metadata first
                st.markdown("**🔍 Record Metadata**")
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.write(f"**Confidence Score:** {record['confidence_score']:.3f}")
                with col2:
                    st.write(f"**Source Patient IDs:** {record['source_patient_ids']}")
                with col3:
                    st.write(f"**Current Status:** {record['steward_status'].upper()}")
                
                # Show existing comments if any
                if record['steward_comments'] and record['steward_comments'] != 'None':
                    st.info(f"**Previous Comments:** {record['steward_comments']}")
                
                # Enhanced stewardship interface for pending records
                if record['steward_status'] == 'pending':
                    show_enhanced_stewardship_interface(record, steward_name, user_token)
                
                # Show approval info for approved/rejected records
                elif record['steward_status'] in ['approved', 'rejected']:
                    st.markdown("**✅ Stewardship Decision**")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Status:** {record['steward_status'].upper()}")
                        st.write(f"**Approved By:** {record['approved_by']}")
                    with col2:
                        st.write(f"**Decision Date:** {record['approved_at']}")
                        st.write(f"**Comments:** {record['steward_comments']}")
                    
                    # Show final golden record values
                    st.markdown("**📋 Final Golden Record**")
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("**Patient Information**")
                        st.write(f"**Name:** {record['patient_name']}")
                        st.write(f"**MRN:** {record['medical_record_num']}")
                        st.write(f"**DOB:** {record['date_of_birth']}")
                        st.write(f"**Medicare:** {record['medicare_number']}")
                        st.write(f"**Phone:** {record['phone']}")
                        st.write(f"**Email:** {record['email']}")
                        st.write(f"**Blood Type:** {record['blood_type']}")
                        st.write(f"**Gender:** {record['gender']}")
                    
                    with col2:
                        st.markdown("**Address & Healthcare**")
                        st.write(f"**Address:** {record['address']}")
                        st.write(f"**Suburb:** {record['suburb']}")
                        st.write(f"**State:** {record['state']}")
                        st.write(f"**Postcode:** {record['postcode']}")
                        st.write(f"**Health Fund:** {record['private_health_fund']}")
                        st.write(f"**Membership:** {record['membership_number']}")
                        st.write(f"**GP:** {record['gp_name']}")
                        st.write(f"**Emergency Contact:** {record['emergency_contact']}")
    
    else:
        st.info("No records match the selected filter criteria.")

def show_database_config():
    """Display database configuration page"""
    st.header("⚙️ Database Configuration")
    
    st.markdown("""
    Configure the database connection parameters and AI model settings for your Healthcare MDM system.
    These settings determine which catalog, schema, tables, and AI model the application will use.
    """)
    
    # Database Configuration Form
    with st.form("db_config_form"):
        st.subheader("📊 Database Settings")
        
        col1, col2 = st.columns(2)
        
        with col1:
            catalog_name = st.text_input(
                "Catalog Name", 
                value=st.session_state.db_config['catalog_name'],
                help="The Unity Catalog name (e.g., 'main', 'dev', 'prod')"
            )
            
            schema_name = st.text_input(
                "Schema Name", 
                value=st.session_state.db_config['schema_name'],
                help="The schema/database name within the catalog"
            )
        
        with col2:
            table_name = st.text_input(
                "Source Table Name", 
                value=st.session_state.db_config['table_name'],
                help="The main patient data table name"
            )
            
            golden_table_name = st.text_input(
                "Golden Records Table Name", 
                value=st.session_state.db_config['golden_table_name'],
                help="The table name for approved golden records"
            )
        
        db_submitted = st.form_submit_button("💾 Save Database Configuration", type="primary")
        
        if db_submitted:
            # Update session state
            st.session_state.db_config = {
                'catalog_name': catalog_name,
                'schema_name': schema_name,
                'table_name': table_name,
                'golden_table_name': golden_table_name
            }
            st.success("✅ Database configuration updated successfully!")
            st.rerun()
    
    # AI Model Configuration Form
    st.markdown("---")
    with st.form("ai_config_form"):
        st.subheader("🤖 AI Model Settings")
        
        model_options = [
            'databricks-meta-llama-3-3-70b-instruct',
            'databricks-gpt-oss-120b',
            'databricks-gpt-oss-20b',
            'databricks-claude-3-7-sonnet',
            'databricks-claude-sonnet-4'
        ]
        
        current_model = st.session_state.ai_config['model_name']
        if current_model not in model_options:
            model_options.append(current_model)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            model_name = st.selectbox(
                "AI Model", 
                options=model_options,
                index=model_options.index(current_model),
                help="The AI model to use for duplicate detection, quality assessment, and golden record generation"
            )
        
        with col2:
            st.markdown("**Model Info**")
            if 'llama-3-3-70b' in model_name:
                st.info("🦙 Llama 3.3 70B - Latest, balanced performance (Default)")
            elif 'gpt-oss-120b' in model_name:
                st.info("🤖 GPT OSS 120B - Large open-source model")
            elif 'gpt-oss-20b' in model_name:
                st.info("🤖 GPT OSS 20B - Efficient open-source model")
            elif 'claude-3-7-sonnet' in model_name:
                st.info("🎭 Claude 3.7 Sonnet - Advanced reasoning")
            elif 'claude-sonnet-4' in model_name:
                st.info("🎭 Claude Sonnet 4 - Latest Claude model")
            else:
                st.info("ℹ️ Custom model configuration")
        
        ai_submitted = st.form_submit_button("🤖 Save AI Configuration", type="secondary")
        
        if ai_submitted:
            # Update session state
            st.session_state.ai_config = {
                'model_name': model_name
            }
            st.success("✅ AI model configuration updated successfully!")
            st.rerun()
    
    # Display current configuration
    st.subheader("📋 Current Configuration")
    db_config = st.session_state.db_config
    ai_config = st.session_state.ai_config
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.info(f"""
        **Source Table:** 
        `{db_config['catalog_name']}.{db_config['schema_name']}.{db_config['table_name']}`
        """)
    
    with col2:
        st.info(f"""
        **Golden Records Table:** 
        `{db_config['catalog_name']}.{db_config['schema_name']}.{db_config['golden_table_name']}`
        """)
    
    with col3:
        st.info(f"""
        **AI Model:** 
        `{ai_config['model_name']}`
        """)
    
    # Test connection
    st.subheader("🔍 Test Connection")
    if st.button("Test Database Connection"):
        try:
            # Try to query the source table
            table_ref = get_table_reference('main')
            test_query = f"SELECT COUNT(*) as record_count FROM {table_ref} LIMIT 1"
            
            user_token = st.context.headers.get('X-Forwarded-Access-Token')
            if user_token:
                result = sql_query_with_user_token(test_query, user_token)
            else:
                result = sql_query_with_service_principal(test_query)
            
            if not result.empty:
                record_count = result.iloc[0]['record_count']
                st.success(f"✅ Connection successful! Found {record_count} records in source table.")
            else:
                st.warning("⚠️ Connection successful but no data found.")
                
        except Exception as e:
            st.error(f"❌ Connection failed: {str(e)}")

# Main application
def main():
    # Extract user access token from the request headers
    user_token = st.context.headers.get('X-Forwarded-Access-Token')
    
    # Sidebar navigation
    st.sidebar.title("🔍 Navigation")
    
    page = st.sidebar.selectbox(
        "Choose a page",
        ["📊 Overview", "👥 Patient Records", "🔄 Duplicate Detection", "📈 Data Quality", "👨‍💼 Data Stewardship", "⚙️ Database Config", "🔧 Settings"]
    )
    
    # Cache management in sidebar
    st.sidebar.markdown("---")
    st.sidebar.subheader("⚡ Performance")
    
    # Show cache status
    config = st.session_state.db_config
    cache_keys = [
        f"fetch_patient_data_{config['catalog_name']}_{config['schema_name']}_{config['table_name']}",
        f"fetch_quality_data_{config['catalog_name']}_{config['schema_name']}_{config['table_name']}",
        f"fetch_duplicate_data_{config['catalog_name']}_{config['schema_name']}_{config['table_name']}"
    ]
    
    cached_count = sum(1 for key in cache_keys if is_cache_valid(key))
    st.sidebar.info(f"📋 Cached datasets: {cached_count}/3")
    
    if st.sidebar.button("🔄 Refresh All Data"):
        clear_cache()
        st.rerun()
    
    if st.sidebar.button("🗑️ Clear Cache"):
        clear_cache()
        st.sidebar.success("Cache cleared!")
        st.rerun()
    
    # Load data based on authentication method with async loading
    patient_data = None
    quality_data = None
    duplicate_data = None
    
    # Create loading placeholders
    loading_placeholder = st.empty()
    
    try:
        with loading_placeholder.container():
            # Show cache status
            config = st.session_state.db_config
            cache_key_patient = f"fetch_patient_data_{config['catalog_name']}_{config['schema_name']}_{config['table_name']}"
            cache_key_quality = f"fetch_quality_data_{config['catalog_name']}_{config['schema_name']}_{config['table_name']}"
            cache_key_duplicate = f"fetch_duplicate_data_{config['catalog_name']}_{config['schema_name']}_{config['table_name']}"
            
            cached_patient = is_cache_valid(cache_key_patient)
            cached_quality = is_cache_valid(cache_key_quality)
            cached_duplicate = is_cache_valid(cache_key_duplicate)
            
            if cached_patient and cached_quality and cached_duplicate:
                st.info("📋 Loading data from cache...")
            else:
                st.info("🔄 Loading fresh data from Databricks...")
            
            # Load data based on current page needs
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Always load patient data (needed by most pages)
            status_text.text("Loading patient records...")
            progress_bar.progress(10)
            if user_token:
                patient_data = fetch_patient_data(user_token)
            else:
                patient_data = fetch_patient_data()
            progress_bar.progress(40)
            
            # Load quality data only when needed
            if page in ["📈 Data Quality", "📊 Overview"]:
                status_text.text("Analyzing data quality...")
                progress_bar.progress(50)
                if user_token:
                    quality_data = fetch_quality_data(user_token)
                else:
                    quality_data = fetch_quality_data()
                progress_bar.progress(70)
            else:
                quality_data = generate_demo_quality_data()  # Use demo data for other pages
                progress_bar.progress(70)
            
            # Load duplicate data only when needed
            if page in ["🔄 Duplicate Detection", "📊 Overview", "👨‍💼 Data Stewardship"]:
                status_text.text("Processing duplicate detection...")
                progress_bar.progress(80)
                if user_token:
                    duplicate_data = fetch_duplicate_data(user_token)
                else:
                    duplicate_data = fetch_duplicate_data()
                progress_bar.progress(100)
            else:
                duplicate_data = pd.DataFrame()
                progress_bar.progress(100)
            
            status_text.text("✅ Data loading complete!")
            time.sleep(0.5)  # Brief pause to show completion
            progress_bar.empty()
            status_text.empty()
        
        # Clear loading placeholder
        loading_placeholder.empty()
        
    except Exception as e:
        loading_placeholder.empty()
        st.error(f"Error loading data: {str(e)}")
        patient_data = generate_demo_data()
        quality_data = generate_demo_quality_data()
        duplicate_data = pd.DataFrame()
    
    # Display selected page
    if page == "📊 Overview":
        show_overview_dashboard(patient_data, quality_data, duplicate_data)
    elif page == "👥 Patient Records":
        show_patient_records(patient_data)
    elif page == "🔄 Duplicate Detection":
        show_duplicate_detection(duplicate_data)
    elif page == "📈 Data Quality":
        show_data_quality(quality_data)
    elif page == "👨‍💼 Data Stewardship":
        show_data_stewardship(user_token)
    elif page == "⚙️ Database Config":
        show_database_config()
    elif page == "🔧 Settings":
        st.header("🔧 System Settings")
        st.info("Settings page - Configure system parameters here")
        
        # Display connection info
        st.subheader("🔗 Connection Information")
        st.write(f"**Warehouse ID**: {os.getenv('DATABRICKS_WAREHOUSE_ID', 'Not configured')}")
        st.write(f"**Host**: {cfg.host if hasattr(cfg, 'host') else 'Not available'}")
        st.write(f"**Authentication**: {'User Token' if user_token else 'Service Principal'}")
        
        # Display current database configuration
        st.subheader("📊 Current Database Configuration")
        config = st.session_state.db_config
        st.json(config)

if __name__ == "__main__":
    main()
