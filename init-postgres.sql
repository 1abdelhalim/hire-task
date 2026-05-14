-- PostgreSQL initialisation script
-- Creates the fact table (without FK constraints for demo simplicity)
-- and inserts the sample data for testing.

CREATE TABLE IF NOT EXISTS app_user_visits_fact (
    id text NOT NULL PRIMARY KEY,
    phone_number text NULL,
    seen int NULL,
    state int NULL,
    points float8 NULL,
    receipt float8 NULL,
    "countryCode" text NULL,
    remaining float8 NULL,
    customer_id text NOT NULL,
    branch_id text NOT NULL,
    store_id text NOT NULL,
    cashier_id text NOT NULL,
    created_at int8 NULL,
    updated_at int8 NULL,
    expired int NULL,
    expires_at int8 NULL,
    order_id text NULL,
    is_deleted int2 DEFAULT 0 NOT NULL,
    is_fraud int2 DEFAULT 0 NOT NULL,
    sync_mechanism text NULL,
    is_bulk_points text NULL
);

INSERT INTO app_user_visits_fact (id,phone_number,seen,state,points,receipt,"countryCode",remaining,customer_id,branch_id,store_id,cashier_id,created_at,updated_at,expired,expires_at,order_id,is_deleted,is_fraud,sync_mechanism,is_bulk_points) VALUES
    ('92128927-eaf7-4e31-9269-7f8c38e4d1cc','0550693380',1,2,11.0,11.64,'+966',0.0,'935a56d5-6ba5-4afa-a021-5f68ca979d95','b9e6030c-e493-44a7-b703-a43d7bc581b0','cc379a3a-3587-4c90-90d2-fecaf0c17280','bdef37c4-be15-4599-970f-c8cf953c74bd',1758628843010,1759341646369,NULL,NULL,'bcd80a6f-b99b-4b91-85fb-7fd460f216c8',0,0,'messaging_queue',''),
    ('dbb6b06a-dd5c-42a5-a9dc-606a11ddc6b8','0552377277',NULL,2,13.0,13.0,'+966',13.0,'5ff3a4bb-2fcc-4c8a-b180-92e6e069f511','65051777-6d0c-43a9-b964-8a9bcb6a26df','3f4cd978-90d3-4b0f-aac1-0703a7600e0a','7b087e6f-cb9a-4017-8dcb-ac6f8284165e',1753583922000,1753583922000,NULL,NULL,NULL,0,0,'messaging_queue',''),
    ('9b9f9b72-65ef-414a-9a30-026cefe29b96','0534923005',NULL,2,1.0,1.0,'+966',1.0,'cc7ff199-dfaf-435f-b0dc-76fa27e62bd4','c2297419-a4e7-40a0-9ea6-99ed88709fe1','3f4cd978-90d3-4b0f-aac1-0703a7600e0a','6c53ccda-720c-429e-89d2-3160f7512744',1753584115000,1753584115000,NULL,NULL,NULL,0,0,'messaging_queue',''),
    ('0b4b4a83-2c37-4fae-8f10-9cdcf4403e70','0550693380',1,2,9.0,9.54,'+966',0.0,'935a56d5-6ba5-4afa-a021-5f68ca979d95','abb72289-ab34-4920-a306-387cda5aa3de','cc379a3a-3587-4c90-90d2-fecaf0c17280','1e6098ca-1167-4973-aca2-4cc99558fa0d',1758736479787,1759341646479,NULL,NULL,'27a4b82a-ae78-4c9d-a261-cddc85885576',0,0,'messaging_queue',''),
    ('6c30b4c4-8d75-4ab6-98d9-fef684ad14bc','0567224428',NULL,2,9.0,9.0,'+966',9.0,'8e5a4e93-f158-42a3-9651-adbb404b249d','66abb54f-da01-4e0b-b6a8-6734b08add61','3f4cd978-90d3-4b0f-aac1-0703a7600e0a','ecfc7a86-c576-4df0-8e35-bb2836b66eff',1753584843000,1753584843000,NULL,NULL,NULL,0,0,'messaging_queue',''),
    ('6af6e8a1-4250-4f4e-ab62-3fb60853c275','0534412326',NULL,2,12.0,12.0,'+966',12.0,'b5504773-ce7d-4177-8190-9c39d300617b','ef44609b-0398-47f3-b75f-931e6bf80588','6196ed70-2b28-4fec-9086-572422c628db','f7eae5b0-a200-4063-8172-5caa59647dad',1753584939405,1753584939405,NULL,NULL,'36c1ca1a-a101-4a88-a7fc-2da6328e398f',0,0,'messaging_queue',''),
    ('dafe8f73-f6e5-4a21-8bb4-284ee90724d2','0502499988',1,2,9.0,9.5,'+966',0.0,'c2501f04-8984-4a4f-8b61-e7a8aad7369b','96e5dd2e-5d7b-48ed-a2dc-94fef69c2336','bd8d6958-d8dd-43a8-9f5c-b793503963d9','df9ade7d-0944-4543-8364-eedebc8e4167',1753584471000,1755340936156,NULL,NULL,NULL,0,0,'messaging_queue',''),
    ('b2ca2fc9-46eb-4a37-aa34-a8ecde9712bd','0550759954',1,2,41.0,41.0,'+966',41.0,'6a4419f7-79a0-4d0b-b586-dec253f8e842','e20cee38-3b62-4a28-9d57-ad2995e61570','829fb362-d9d8-4808-a034-1b21154efdbe','d6f55ddf-4b48-4bca-9c66-bd02ae94e18e',1753584614000,1756180802888,1,1756176614000,NULL,0,0,'messaging_queue',''),
    ('5bfb3f5d-4160-49e2-9e20-0d4b64786537','0550693380',1,2,11.0,11.93,'+966',0.0,'935a56d5-6ba5-4afa-a021-5f68ca979d95','abb72289-ab34-4920-a306-387cda5aa3de','cc379a3a-3587-4c90-90d2-fecaf0c17280','1e6098ca-1167-4973-aca2-4cc99558fa0d',1758914040646,1759341646653,NULL,NULL,'154241ee-b942-4ce0-b7bc-a1e3d3316ec8',0,0,'messaging_queue',''),
    ('89ea0a7a-52ef-4290-b681-c4463db4d35c','0550693380',1,2,7.0,7.46,'+966',2.0,'935a56d5-6ba5-4afa-a021-5f68ca979d95','abb72289-ab34-4920-a306-387cda5aa3de','cc379a3a-3587-4c90-90d2-fecaf0c17280','1e6098ca-1167-4973-aca2-4cc99558fa0d',1759173190174,1759341646839,NULL,NULL,'4d620ec1-23ac-4f28-87d1-72fa9df8d3f8',0,0,'messaging_queue',''),
    ('8a2ce7fd-9d4f-4094-a66c-bcd23b1e3d22','0503870494',1,2,18.0,18.0,'+966',18.0,'be906186-1f8b-4270-8968-a62af7ea8021','d1f6633f-7d8c-42ed-a658-9b14c26fd845','a6b9b06e-64b8-476d-b608-5edafe5a0162','60363490-0294-47d6-a9d2-b08a8345b12e',1759342634906,1759342634906,NULL,NULL,NULL,0,0,'messaging_queue',''),
    ('edfa665c-38d3-4828-b491-2cd0d2bf5c09','0598204291',NULL,2,54.0,54.0,'+966',54.0,'5ef2b5f1-8071-4058-a708-dec9a976732a','ad4d442a-b31c-4507-b01f-535a71ca72d3','c9838c81-b7c8-4050-b7a3-5647cf1f9931','263705c1-c376-4405-b67e-b62630557f84',1759341647832,1759341647832,0,1767117647832,'2232b301-5d38-43fe-94c4-71d7c7bc0677',0,0,'messaging_queue',NULL),
    ('0cf1e2b9-fb03-47d2-94fa-57e176fddd72','0569541056',NULL,2,51.0,51.2,'+966',51.0,'e451f0c7-c11a-46af-a58e-8c177f5006d4','ed2deafc-96ae-4af1-9497-da6d1d991f22','d40822be-06d8-447b-8c25-57d014095240','1b43875f-2a1c-4b69-bf58-9a5e0e8ba595',1759341647923,1759341647923,0,1761933647923,'73d19730-2226-425d-aeea-113ae11ff9b1',0,0,'messaging_queue',NULL),
    ('f34df69a-3838-4807-b318-e025adccd9d3','0558502667',NULL,2,30.0,30.0,'+966',30.0,'be34ee84-5d3c-4da0-91d8-00d58b000741','11662314-6781-465b-91c5-b57acea65c53','fe73e9a9-e964-4bff-b99a-2faf010a3e5f','8fbf5c7c-cb53-42eb-8c7f-c331cf3bb975',1759341648011,1759341648011,NULL,NULL,'6262a0e2-e747-4edc-985c-3779f03dd1d3',0,0,'messaging_queue',NULL),
    ('ebb02159-e663-456e-9909-232dd6db417b','0533674481',NULL,2,22.0,22.0,'+966',22.0,'c2dcedf9-6d9a-4bc4-8a25-8617d9b529a0','6128329a-0318-4a08-9886-444fe89fcefe','34ff35ef-cb57-4521-9c9c-e1bd96874f21','d64fe0f9-3cf9-4489-a27e-7100b5df7866',1759356187000,1759356187000,NULL,NULL,NULL,0,0,'messaging_queue',NULL),
    ('18b35e12-8eda-4de2-a942-897c2cd3e7da','0569733568',NULL,2,4.0,4.9,'+966',4.0,'cc1c72c3-f22f-47c8-a9c4-b936a94125ce','a1d091ea-224f-41ea-80bc-d70500dbd873','b90f4c8d-7938-4893-a8be-6aeb99d05b2f','5e634c60-2227-4cd4-adee-f24ca6b4305e',1759356318000,1759356318000,NULL,NULL,NULL,0,0,'messaging_queue',NULL);
