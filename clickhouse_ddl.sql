CREATE TABLE IF NOT EXISTS default.app_user_visits_fact
(
    id String,
    phone_number Nullable(String),
    seen Nullable(Int32),
    state Nullable(Int32),
    points Nullable(Float64),
    receipt Nullable(Float64),
    countryCode Nullable(String),
    remaining Nullable(Float64),
    customer_id String,
    branch_id String,
    store_id String,
    cashier_id String,
    created_at Nullable(Int64),
    updated_at Int64 DEFAULT 0,
    expired Nullable(Int32),
    expires_at Nullable(Int64),
    order_id Nullable(String),
    is_deleted Int16 DEFAULT 0,
    is_fraud Int16 DEFAULT 0,
    sync_mechanism Nullable(String),
    is_bulk_points Nullable(String)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id;
