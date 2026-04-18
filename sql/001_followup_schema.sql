BEGIN;

CREATE TABLE IF NOT EXISTS selection_snapshots (
    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    object_id TEXT NOT NULL,
    candid TEXT NOT NULL,
    topic TEXT NOT NULL,
    stage TEXT NOT NULL,                      -- pre_report / reported / reevaluation
    selected_utc TEXT NOT NULL,               -- ISO8601 UTC
    selector_version TEXT NOT NULL,           -- ej: n1_hostless_fast@v1.2.0
    cfg_hash TEXT NOT NULL,                   -- hash del YAML o de params efectivos
    passed_n1 INTEGER NOT NULL CHECK (passed_n1 IN (0,1)),
    passed_pre_report_gate INTEGER NOT NULL CHECK (passed_pre_report_gate IN (0,1)),
    gate_reason TEXT NOT NULL,
    magpsf REAL,
    fid INTEGER,
    drb REAL,
    rb REAL,
    isdiffpos TEXT,
    ndethist INTEGER,
    days_since_nondet REAL,
    delta_mag_from_nondet REAL,
    nmtchps INTEGER,
    distpsnr1 REAL,
    srmag1 REAL,
    ssdistnr REAL,
    pre_report_score REAL,
    features_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_selection_snapshots_obj_cand
ON selection_snapshots(object_id, candid);

CREATE INDEX IF NOT EXISTS idx_selection_snapshots_stage_time
ON selection_snapshots(stage, selected_utc DESC);


CREATE TABLE IF NOT EXISTS tns_report_state (
    report_id TEXT PRIMARY KEY,
    object_id TEXT NOT NULL,
    candid TEXT NOT NULL,
    submitted_utc TEXT NOT NULL,
    submit_status TEXT NOT NULL,              -- submitted / failed / unknown
    tns_name TEXT,                            -- ej: AT 2026abc
    public_utc TEXT,
    reply_status TEXT NOT NULL DEFAULT 'pending',  -- pending / resolved / failed
    classification_status TEXT NOT NULL DEFAULT 'unknown', -- unknown / classified / classified_by_me / classified_by_others
    tns_url TEXT,
    certificate_url TEXT,
    last_checked_utc TEXT,
    raw_reply_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_tns_report_state_obj_cand
ON tns_report_state(object_id, candid);

CREATE INDEX IF NOT EXISTS idx_tns_report_state_tns_name
ON tns_report_state(tns_name);


CREATE TABLE IF NOT EXISTS followup_queue (
    queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
    object_id TEXT NOT NULL,
    candid TEXT NOT NULL,
    report_id TEXT,
    tns_name TEXT,
    topic TEXT NOT NULL,
    submitted_utc TEXT NOT NULL,
    status TEXT NOT NULL,                     -- watch / watch_high / promote_photometry / promote_spectroscopy / observed_photometry / observed_spectrum / classified / classified_by_others / dropped
    priority_bucket TEXT NOT NULL DEFAULT 'normal',  -- low / normal / high / urgent
    followup_owner TEXT,
    current_score REAL,
    best_score REAL,
    last_score_utc TEXT,
    promotion_triggered INTEGER NOT NULL DEFAULT 0 CHECK (promotion_triggered IN (0,1)),
    promotion_utc TEXT,
    promotion_reason TEXT,
    dropped_reason TEXT,
    external_classification INTEGER NOT NULL DEFAULT 0 CHECK (external_classification IN (0,1)),
    external_classification_label TEXT,
    next_review_utc TEXT,
    last_review_utc TEXT,
    notes TEXT,
    UNIQUE(object_id, candid)
);

CREATE INDEX IF NOT EXISTS idx_followup_queue_status
ON followup_queue(status);

CREATE INDEX IF NOT EXISTS idx_followup_queue_next_review
ON followup_queue(next_review_utc);

CREATE INDEX IF NOT EXISTS idx_followup_queue_score
ON followup_queue(current_score DESC);


CREATE TABLE IF NOT EXISTS followup_score_history (
    score_id INTEGER PRIMARY KEY AUTOINCREMENT,
    object_id TEXT NOT NULL,
    candid TEXT NOT NULL,
    score_utc TEXT NOT NULL,
    score_version TEXT NOT NULL,              -- ej: classifiability_v1
    brightness_score REAL NOT NULL,
    freshness_score REAL NOT NULL,
    evolution_score REAL NOT NULL,
    observability_score REAL NOT NULL,
    field_cleanliness_score REAL NOT NULL,
    hostless_cleanliness_score REAL NOT NULL,
    external_status_score REAL NOT NULL,
    total_score REAL NOT NULL,
    current_mag REAL,
    current_fid INTEGER,
    days_since_nondet REAL,
    mag_slope_per_day REAL,
    max_alt_deg REAL,
    hours_above_35deg REAL,
    moon_sep_deg REAL,
    nmtchps INTEGER,
    distpsnr1 REAL,
    srmag1 REAL,
    tns_name TEXT,
    score_breakdown_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_followup_score_history_obj_time
ON followup_score_history(object_id, candid, score_utc DESC);

CREATE INDEX IF NOT EXISTS idx_followup_score_history_total
ON followup_score_history(total_score DESC);


CREATE TABLE IF NOT EXISTS followup_actions (
    action_id INTEGER PRIMARY KEY AUTOINCREMENT,
    object_id TEXT NOT NULL,
    candid TEXT NOT NULL,
    action_utc TEXT NOT NULL,
    actor TEXT NOT NULL,                      -- system / user
    action_type TEXT NOT NULL,                -- score_update / status_change / promote / drop / request_photometry / request_spectroscopy / classify / note
    old_status TEXT,
    new_status TEXT,
    action_reason TEXT NOT NULL,
    payload_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_followup_actions_obj_time
ON followup_actions(object_id, candid, action_utc DESC);

CREATE INDEX IF NOT EXISTS idx_followup_actions_type
ON followup_actions(action_type, action_utc DESC);


CREATE TABLE IF NOT EXISTS followup_observations (
    obs_id INTEGER PRIMARY KEY AUTOINCREMENT,
    object_id TEXT NOT NULL,
    candid TEXT,
    tns_name TEXT,
    obs_utc TEXT NOT NULL,
    source TEXT NOT NULL,                     -- own / rented / public / archive
    facility TEXT,
    instrument TEXT,
    observer TEXT,
    obs_type TEXT NOT NULL,                   -- photometry / spectroscopy / limit
    band TEXT,
    mag REAL,
    magerr REAL,
    limit_mag REAL,
    exposure_s REAL,
    snr REAL,
    spectrum_path TEXT,
    spectrum_url TEXT,
    reduced_by TEXT,
    classification_label TEXT,
    classification_confidence REAL,
    raw_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_followup_observations_obj_time
ON followup_observations(object_id, obs_utc DESC);

CREATE INDEX IF NOT EXISTS idx_followup_observations_type
ON followup_observations(obs_type, obs_utc DESC);

COMMIT;