-- Databricks notebook source
-- DBTITLE 1,Initialize Catalog and Create Healthcare MDM Schema
USE CATALOG danny_catalog;
CREATE SCHEMA IF NOT EXISTS healthcare_mdm_schema;

-- COMMAND ----------

-- DBTITLE 1,Switch to Healthcare MDM Schema
USE SCHEMA healthcare_mdm_schema;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Synthetic Healthcare Data
-- MAGIC
-- MAGIC This healthcare MDM example demonstrates:
-- MAGIC - **Patient Record Deduplication**: Identifying duplicate patient records across different healthcare systems
-- MAGIC - **Provider Data Quality**: Assessing healthcare provider information completeness and accuracy
-- MAGIC - **Golden Record Creation**: Creating master patient and provider records from multiple sources
-- MAGIC - **Healthcare Matching**: Considering Medicare numbers, medical record numbers, and clinical data

-- COMMAND ----------

-- DBTITLE 1,Define Patient Table with Australian Healthcare-Specific Fields
CREATE TABLE patients (
    patient_id          BIGINT PRIMARY KEY,
    medical_record_num  STRING,
    patient_name        STRING,
    date_of_birth       DATE,
    medicare_number     STRING,
    phone               STRING,
    email               STRING,
    address             STRING,
    suburb              STRING,
    state               STRING,
    postcode            STRING,
    private_health_fund STRING,
    membership_number   STRING,
    emergency_contact   STRING,
    gp_name             STRING,  -- General Practitioner
    blood_type          STRING,
    gender              STRING,
    source_system       STRING
);


-- COMMAND ----------

INSERT INTO patients (patient_id, medical_record_num, patient_name, date_of_birth, medicare_number, phone, email, address, suburb, state, postcode, private_health_fund, membership_number, emergency_contact, gp_name, blood_type, gender, source_system) VALUES
    (1, 'MRN001234', 'John Smith', '1985-03-15', '2428 9123 4567 8', '02-9123-4567', 'john.smith@email.com', '123 Collins Street', 'Melbourne', 'VIC', '3000', 'Medibank', 'MB123456789', 'Mary Smith (02-9123-4568)', 'Dr. Sarah Johnson', 'O+', 'M', 'EMR_System_A'),
    (2, 'MR001234', 'J. Smith', '1985-03-15', '2428912345678', '02-9123-4567', 'j.smith@email.com', '123 Collins St', 'Melbourne', 'VIC', '3000', 'Medibank Private', 'MB123456789', 'Mary Smith', 'Dr. Sarah Johnson', 'O+', 'M', 'EMR_System_B'),
    (3, 'MRN005678', 'Jane Doe', '1990-07-22', '2987 6543 2109 8', '03-9876-5432', 'jane.doe@email.com', '456 Swanston Street', 'Carlton', 'VIC', '3053', 'BUPA', 'BP987654321', 'Robert Doe (03-9876-5433)', 'Dr. Michael Williams', 'A-', 'F', 'EMR_System_A'),
    (4, 'MRN001234', 'Jon Smythe', '1985-03-15', '2428 9123 4567 8', '02-9123-4567', 'jon.smythe@email.com', '123 Collins Street', 'Melbourne', 'VIC', '3000', 'Medibank', 'MB123456789', 'Mary Smythe', 'Dr. Sarah Johnson', 'O+', 'M', 'Lab_System'),
    (5, 'MRN005679', 'Janet Doe', '1990-07-22', '2987654321098', '03-9321-6547', 'janet.d@email.com', '456 Swanston St', 'Carlton', 'VIC', '3053', 'BUPA Health', 'BP987654322', 'Robert Doe', 'Dr. Michael Williams', 'A-', 'F', 'Billing_System'),
    (6, 'MR001234', 'John Smith', '1985-03-15', NULL, NULL, 'john.s@email.com', '123 Collins St.', 'Melbourne', 'VIC', '3000', 'Medibank', 'MB123456789', 'Mary Smith', 'Dr. Sarah Johnson', NULL, 'M', 'Registration_System'),
    (7, 'MRN009876', 'Jake Johnson', '1978-11-08', '2456 7890 1234 5', '07-3222-3333', 'jake.j@email.com', '789 Queen Street', 'Brisbane', 'QLD', '4000', 'HCF', 'HCF123456789', 'Lisa Johnson (07-3222-3334)', 'Dr. Peter Brown', 'B+', 'M', 'EMR_System_A'),
    (8, 'MRN005678', 'Janie Doe', '1990-07-22', '2987 6543 2109 8', '03-9876-5432', 'janed@email.com', '456 Swanston Street', 'Carlton', 'VIC', '3053', 'BUPA', 'BP987654321', 'Robert Doe', 'Dr. Michael Williams', 'A-', 'F', 'Pharmacy_System');

-- COMMAND ----------

INSERT INTO patients (patient_id, medical_record_num, patient_name, date_of_birth, medicare_number, phone, email, address, suburb, state, postcode, private_health_fund, membership_number, emergency_contact, gp_name, blood_type, gender, source_system) VALUES
    (9,  'MRN012345', 'William Turner', '1975-12-03', '2345 6789 0123 4', '08-8888-1111', 'william.turner@email.com', '789 Rundle Street', 'Adelaide', 'SA', '5000', 'NIB', 'NIB123456789', 'Susan Turner (08-8888-1112)', 'Dr. Emma Davis', 'AB+', 'M', 'EMR_System_A'),
    (10, 'MRN067890', 'Linda Carter', '1982-05-18', '2234 5678 9012 3', '02-8333-9753', 'linda.c@email.com', '1001 George Street', 'Sydney', 'NSW', '2000', 'AHM', 'AHM987654321', 'Michael Carter (02-8333-9754)', 'Dr. James Wilson', 'B-', 'F', 'EMR_System_B'),
    (11, 'MRN098765', 'Pedro Alvarez', '1988-09-25', '2567 8901 2345 6', '08-9777-5533', 'palvarez@email.com', '22 Hay Street', 'Perth', 'WA', '6000', 'HBF', 'HBF123456789', 'Maria Alvarez (08-9777-5534)', 'Dr. Carlos Martinez', 'O-', 'M', 'Billing_System'),
    (12, 'MRN054321', 'Amy Nguyen', '1993-01-12', '2678 9012 3456 7', '03-8111-2222', 'amy.n@email.com', '321 Bourke Street', 'Melbourne', 'VIC', '3000', 'Teachers Health', 'TH123456789', 'David Nguyen (03-8111-2223)', 'Dr. Lisa Thompson', 'A+', 'F', 'EMR_System_A'),
    (13, 'MRN087654', 'Marcus Lee', '1980-06-30', '2789 0123 4567 8', '03-9121-3344', 'mlee@email.com', '501 Chapel Street', 'South Yarra', 'VIC', '3141', 'Australian Unity', 'AU123456789', 'Jennifer Lee (03-9121-3345)', 'Dr. Robert Anderson', 'B+', 'M', 'Lab_System'),
    (14, 'MRN076543', 'Sara Patel', '1987-04-14', '2890 1234 5678 9', '08-8676-7812', 'sara.patel@email.com', '48 King William Street', 'Adelaide', 'SA', '5000', 'Defence Health', 'DH123456789', 'Raj Patel (08-8676-7813)', 'Dr. Helen Clark', 'AB-', 'F', 'EMR_System_B'),
    (15, 'MRN065432', 'David Kim', '1991-08-07', '2901 2345 6789 0', '08-9909-3030', 'david.kim@email.com', '650 Murray Street', 'Perth', 'WA', '6000', 'Police Health', 'PH123456789', 'Sarah Kim (08-9909-3031)', 'Dr. Mark Lewis', 'A-', 'M', 'Registration_System'),
    (16, 'MRN054321', 'Elena Garcia', '1986-02-28', '2012 3456 7890 1', '02-8452-1098', 'elena.g@email.com', '700 Pitt Street', 'Sydney', 'NSW', '2000', 'GMHBA', 'GM123456789', 'Carlos Garcia (02-8452-1099)', 'Dr. Maria Rodriguez', 'O+', 'F', 'Pharmacy_System');


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##  Healthcare MDM Use Cases with ai_query()
-- MAGIC
-- MAGIC ### Key Healthcare MDM Challenges:
-- MAGIC - **Patient Identity Resolution**: Multiple systems create duplicate patient records
-- MAGIC - **Data Quality Issues**: Incomplete or inconsistent healthcare data across systems
-- MAGIC - **Clinical Data Integration**: Merging records from EMR, pathology, billing, and pharmacy systems

-- COMMAND ----------

-- DBTITLE 1,Australian Patient Identity Resolution - Duplicate Detection
WITH pairwise_similarity AS (
    SELECT 
        t1.patient_id AS id1,
        t2.patient_id AS id2,
        t1.medical_record_num AS mrn1,
        t2.medical_record_num AS mrn2,
        t1.patient_name AS name1, 
        t2.patient_name AS name2,
        t1.date_of_birth AS dob1,
        t2.date_of_birth AS dob2,
        t1.medicare_number AS medicare1,
        t2.medicare_number AS medicare2,
        t1.phone AS phone1,
        t2.phone AS phone2,
        t1.email AS email1,
        t2.email AS email2,
        t1.address AS address1,
        t2.address AS address2,
        t1.suburb AS suburb1,
        t2.suburb AS suburb2,
        t1.state AS state1,
        t2.state AS state2,
        t1.postcode AS postcode1,
        t2.postcode AS postcode2,
        t1.private_health_fund AS health_fund1,
        t2.private_health_fund AS health_fund2,
        t1.source_system AS system1,
        t2.source_system AS system2,
        ai_query(
            'databricks-meta-llama-3-3-70b-instruct',
            CONCAT(
                'Compare these two patient records and determine if they represent the same person. ',
                'Consider healthcare identifiers: medical record number, Medicare number, date of birth, and demographic data. ',
                'Pay special attention to: exact DOB matches, Medicare number formatting variations, MRN similarities, and address consistency. ',
                'Return your analysis as a JSON object with these keys: similarity_score (number between 0 and 1), is_match (boolean), confidence (string: "low", "medium", "high"), match_reason (string). ',
                'Patient 1: {',
                    'mrn: ', t1.medical_record_num, ', ',
                    'name: ', t1.patient_name, ', ',
                    'dob: ', t1.date_of_birth, ', ',
                    'medicare: ', t1.medicare_number, ', ',

                    'phone: ', t1.phone, ', ',
                    'email: ', t1.email, ', ',
                    'address: ', t1.address, ', ',
                    'suburb: ', t1.suburb, ', ',
                    'state: ', t1.state, ', ',
                    'postcode: ', t1.postcode, ', ',
                    'health_fund: ', t1.private_health_fund, ', ',
                    'source_system: ', t1.source_system,
                '}. ',
                'Patient 2: {',
                    'mrn: ', t2.medical_record_num, ', ',
                    'name: ', t2.patient_name, ', ',
                    'dob: ', t2.date_of_birth, ', ',
                    'medicare: ', t2.medicare_number, ', ',

                    'phone: ', t2.phone, ', ',
                    'email: ', t2.email, ', ',
                    'address: ', t2.address, ', ',
                    'suburb: ', t2.suburb, ', ',
                    'state: ', t2.state, ', ',
                    'postcode: ', t2.postcode, ', ',
                    'health_fund: ', t2.private_health_fund, ', ',
                    'source_system: ', t2.source_system,
                '}. ',
                'Consider healthcare data variations: MRN format differences, Medicare number with/without spaces, IHI variations, name abbreviations, private health fund variations, and cross-system data entry differences. '
            ),
            responseFormat => '{
                "type": "json_schema",
                "json_schema": {
                    "name": "entity_similarity",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "similarity_score": {"type": "number"},
                            "is_match": {"type": "boolean"},
                            "confidence": {"type": "string"},
                            "match_reason": {"type": "string"}
                        },
                        "required": ["similarity_score", "is_match", "confidence", "match_reason"],
                        "strict": true
                    }
                }
            }'
        ) AS similarity_result
    FROM patients t1
    CROSS JOIN patients t2 
    WHERE t1.patient_id < t2.patient_id
)
SELECT
    *,
    similarity_result:similarity_score,
    similarity_result:is_match,
    similarity_result:confidence,
    similarity_result:match_reason
FROM pairwise_similarity
ORDER BY similarity_result:similarity_score DESC
--WHERE similarity_result:is_match = TRUE
;


-- COMMAND ----------

-- DBTITLE 1,Australian Healthcare Data Quality Assessment
WITH patient_quality AS (
    SELECT
        patient_id,
        patient_name,
        source_system,
        ai_query(
            'databricks-meta-llama-3-3-70b-instruct',
            CONCAT(
                'Analyze the data quality of this patient healthcare record. ',
                'Return ONLY a JSON object with these keys and types: quality_score (integer, 0-100), issues (array of strings), completeness (number between 0 and 1). ',
                'Evaluate  requirements: required fields (MRN, DOB, name), data formats (Medicare number, phone, email), clinical data completeness. ',
                'Consider missing critical identifiers like Medicare number as major issues. ',
                'Patient Record: ',
                to_json(struct(
                    medical_record_num,
                    patient_name,
                    date_of_birth,
                    medicare_number,
                    phone,
                    email,
                    address,
                    suburb,
                    state,
                    postcode,
                    private_health_fund,
                    membership_number,
                    emergency_contact,
                    gp_name,
                    blood_type,
                    gender,
                    source_system
                ))
            ),
            responseFormat => '{
                "type": "json_schema",
                "json_schema": {
                    "name": "quality_assessment",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "quality_score": {"type": "integer"},
                            "issues": {"type": "array", "items": {"type": "string"}},
                            "completeness": {"type": "number"}
                        },
                        "required": ["quality_score", "issues", "completeness"],
                        "strict": true
                    }
                }
            }'
        ) AS quality_assessment
    FROM patients
)
SELECT
    patient_id,
    patient_name,
    source_system,
    get_json_object(quality_assessment, '$.quality_score') AS quality_score,
    get_json_object(quality_assessment, '$.issues') AS issues,
    get_json_object(quality_assessment, '$.completeness') AS completeness
FROM patient_quality
ORDER BY CAST(get_json_object(quality_assessment, '$.quality_score') AS INT) DESC
;


-- COMMAND ----------

-- DBTITLE 1,Create Master Patient Records (Golden Records) from Duplicate Detection
WITH pairwise_similarity AS (
    SELECT 
        t1.patient_id AS id1,
        t2.patient_id AS id2,
        t1.medical_record_num AS mrn1,
        t2.medical_record_num AS mrn2,
        t1.patient_name AS name1, 
        t2.patient_name AS name2,
        t1.date_of_birth AS dob1,
        t2.date_of_birth AS dob2,
        t1.medicare_number AS medicare1,
        t2.medicare_number AS medicare2,
        t1.phone AS phone1,
        t2.phone AS phone2,
        t1.email AS email1,
        t2.email AS email2,
        t1.address AS address1,
        t2.address AS address2,
        t1.suburb AS suburb1,
        t2.suburb AS suburb2,
        t1.state AS state1,
        t2.state AS state2,
        t1.postcode AS postcode1,
        t2.postcode AS postcode2,
        t1.private_health_fund AS health_fund1,
        t2.private_health_fund AS health_fund2,
        t1.source_system AS system1,
        t2.source_system AS system2,
        ai_query(
            'databricks-meta-llama-3-3-70b-instruct',
            CONCAT(
                'Compare these two patient records and determine if they represent the same person. ',
                'Consider healthcare identifiers: medical record number, Medicare number, date of birth, and demographic data. ',
                'Return your analysis as a JSON object with these keys: similarity_score (number between 0 and 1), is_match (boolean), confidence (string: "low", "medium", "high"), reasoning (string). ',
                'Patient 1: {',
                    'mrn: ', t1.medical_record_num, ', ',
                    'name: ', t1.patient_name, ', ',
                    'dob: ', t1.date_of_birth, ', ',
                    'medicare: ', t1.medicare_number, ', ',

                    'phone: ', t1.phone, ', ',
                    'email: ', t1.email, ', ',
                    'address: ', t1.address, ', ',
                    'suburb: ', t1.suburb, ', ',
                    'state: ', t1.state, ', ',
                    'postcode: ', t1.postcode, ', ',
                    'health_fund: ', t1.private_health_fund, ', ',
                    'source_system: ', t1.source_system,
                '}. ',
                'Patient 2: {',
                    'mrn: ', t2.medical_record_num, ', ',
                    'name: ', t2.patient_name, ', ',
                    'dob: ', t2.date_of_birth, ', ',
                    'medicare: ', t2.medicare_number, ', ',

                    'phone: ', t2.phone, ', ',
                    'email: ', t2.email, ', ',
                    'address: ', t2.address, ', ',
                    'suburb: ', t2.suburb, ', ',
                    'state: ', t2.state, ', ',
                    'postcode: ', t2.postcode, ', ',
                    'health_fund: ', t2.private_health_fund, ', ',
                    'source_system: ', t2.source_system,
                '}. ',
                'Consider healthcare data variations: MRN format differences, Medicare number with/without spaces, IHI variations, name abbreviations, private health fund variations, and cross-system data entry differences. '
            ),
            responseFormat => '{
                "type": "json_schema",
                "json_schema": {
                    "name": "entity_similarity",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "similarity_score": {"type": "number"},
                            "is_match": {"type": "boolean"},
                            "confidence": {"type": "string"},
                            "reasoning": {"type": "string"}
                        },
                        "required": ["similarity_score", "is_match", "confidence", "reasoning"],
                        "strict": true
                    }
                }
            }'
        ) AS similarity_result,
        -- For golden record creation, provide both patient records as a JSON array
        to_json(
            array(
                struct(t1.patient_id, t1.medical_record_num, t1.patient_name AS name, t1.date_of_birth, t1.medicare_number, t1.email, t1.phone, t1.address, t1.suburb, t1.state, t1.postcode, t1.private_health_fund, t1.membership_number, t1.gp_name, t1.blood_type, t1.gender, t1.source_system),
                struct(t2.patient_id, t2.medical_record_num, t2.patient_name AS name, t2.date_of_birth, t2.medicare_number, t2.email, t2.phone, t2.address, t2.suburb, t2.state, t2.postcode, t2.private_health_fund, t2.membership_number, t2.gp_name, t2.blood_type, t2.gender, t2.source_system)
            )
        ) AS input_pair_json
    FROM patients t1
    CROSS JOIN patients t2 
    WHERE t1.patient_id < t2.patient_id
),
golden_and_similarity AS (
    SELECT
        *,
        -- Build the golden/master patient record for each pair using ai_query()
        ai_query(
            'databricks-meta-llama-3-3-70b-instruct',
            CONCAT(
                'Given these two patient records (possible duplicates), create the most accurate master patient record for healthcare MDM. ',
                'Choose the most complete and accurate values for each field, standardize healthcare formats (MRN, Medicare number), resolve conflicts using healthcare data quality rules. ',
                'Prioritize: most complete MRN, standardized Medicare number format, most recent private health fund info, complete clinical data (blood type, GP). ',
                'Return confidence score (0-1) and list of patient_ids used as sources. ',
                'Patient Records: ', input_pair_json
            ),
            responseFormat => '{
                "type": "json_schema",
                "json_schema": {
                    "name": "golden_record",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "master_record": {
                                "type": "object",
                                "properties": {
                                    "medical_record_num": { "type": "string" },
                                    "name": { "type": "string" },
                                    "date_of_birth": { "type": "string" },
                                    "medicare_number": { "type": "string" },
                                    "phone": { "type": "string" },
                                    "email": { "type": "string" },
                                    "address": { "type": "string" },
                                    "suburb": { "type": "string" },
                                    "state": { "type": "string" },
                                    "postcode": { "type": "string" },
                                    "private_health_fund": { "type": "string" },
                                    "membership_number": { "type": "string" },
                                    "gp_name": { "type": "string" },
                                    "blood_type": { "type": "string" },
                                    "gender": { "type": "string" }
                                },
                                "required": ["medical_record_num", "name", "date_of_birth", "medicare_number", "phone", "email", "address", "suburb", "state", "postcode", "private_health_fund", "membership_number", "gp_name", "blood_type", "gender"]
                            },
                            "confidence": { "type": "number" },
                            "sources": { "type": "array", "items": { "type": "string" } }
                        },
                        "required": ["master_record", "confidence", "sources"],
                        "strict": true
                    }
                }
            }'
        ) AS golden_result
    FROM pairwise_similarity
)
SELECT
    *,
    get_json_object(similarity_result, '$.similarity_score') AS similarity_score,
    get_json_object(similarity_result, '$.is_match') AS is_match,
    get_json_object(similarity_result, '$.confidence') AS confidence,
    get_json_object(similarity_result, '$.reasoning') AS reasoning,
    get_json_object(golden_result, '$.master_record.medical_record_num') AS master_mrn,
    get_json_object(golden_result, '$.master_record.name') AS master_name,
    get_json_object(golden_result, '$.master_record.date_of_birth') AS master_dob,
    get_json_object(golden_result, '$.master_record.medicare_number') AS master_medicare,
    get_json_object(golden_result, '$.master_record.phone') AS master_phone,
    get_json_object(golden_result, '$.master_record.email') AS master_email,
    get_json_object(golden_result, '$.master_record.address') AS master_address,
    get_json_object(golden_result, '$.master_record.suburb') AS master_suburb,
    get_json_object(golden_result, '$.master_record.state') AS master_state,
    get_json_object(golden_result, '$.master_record.postcode') AS master_postcode,
    get_json_object(golden_result, '$.master_record.private_health_fund') AS master_health_fund,
    get_json_object(golden_result, '$.master_record.membership_number') AS master_membership,
    get_json_object(golden_result, '$.master_record.gp_name') AS master_gp,
    get_json_object(golden_result, '$.master_record.blood_type') AS master_blood_type,
    get_json_object(golden_result, '$.master_record.gender') AS master_gender,
    get_json_object(golden_result, '$.confidence') AS master_confidence,
    get_json_object(golden_result, '$.sources') AS master_sources
FROM golden_and_similarity
ORDER BY similarity_score DESC
;
