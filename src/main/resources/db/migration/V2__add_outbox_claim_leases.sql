ALTER TABLE outbox_events
    ADD COLUMN claim_token VARCHAR(36) NULL;

ALTER TABLE outbox_events
    ADD COLUMN claim_until TIMESTAMP NULL;

CREATE INDEX idx_outbox_claim ON outbox_events (published, claim_until);
