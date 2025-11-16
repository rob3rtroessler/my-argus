-- Emails table (Delta Lake)
-- Adjust the database/schema name as needed before running (e.g., `USE my_catalog.my_schema;`)
-- This schema is designed for storing messages synced from Gmail, including headers, recipients,
-- body variants, labels, and attachment metadata.

CREATE TABLE IF NOT EXISTS dev.core.emails (
  -- Stable Gmail identifiers
  email_id STRING NOT NULL,           -- Gmail message id (unique per message)
  thread_id STRING,                   -- Gmail thread id

  -- Header fields
  subject STRING,
  from_name STRING,
  from_email STRING,
  reply_to STRING,
  in_reply_to STRING,                 -- Message-Id this email replies to
  references ARRAY<STRING>,           -- Message-Ids in the References header

  -- Recipients
  to_recipients ARRAY<STRING>,        -- RFC5322 email addresses
  cc_recipients ARRAY<STRING>,
  bcc_recipients ARRAY<STRING>,

  -- Times
  sent_at TIMESTAMP,                  -- Date header
  received_at TIMESTAMP,              -- Server receipt (if available)
  gmail_internal_date TIMESTAMP,      -- Gmail internalDate (ms since epoch) mapped to TIMESTAMP
  received_date DATE,                 -- Derived for partitioning

  -- Content
  snippet STRING,                     -- Gmail snippet
  body_text STRING,                   -- Plaintext body
  body_html STRING,                   -- HTML body
  raw_headers STRING,                 -- Raw headers if you choose to persist them

  -- Labels & flags
  labels ARRAY<STRING>,               -- Gmail label names
  is_read BOOLEAN,
  is_starred BOOLEAN,
  importance STRING,                  -- e.g., 'high' if parsed from headers (optional)
  spam_flag BOOLEAN,                  -- If message is marked as spam

  -- Size/metrics
  message_size_bytes BIGINT,

  -- Attachments
  has_attachments BOOLEAN,
  attachments ARRAY<STRUCT<
    filename STRING,
    mime_type STRING,
    size_bytes BIGINT,
    attachment_id STRING              -- Gmail attachment id for retrieval
  >>,

  -- Gmail change tracking
  gmail_history_id STRING,

  -- Operational metadata
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
USING delta
PARTITIONED BY (received_date)
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5'
);

-- Optional constraints & comments (uncomment to enforce in environments that support them)
-- ALTER TABLE gmail_emails SET TBLPROPERTIES ('quality' = 'silver');
-- ALTER TABLE gmail_emails ALTER COLUMN email_id COMMENT 'Gmail message id';
-- ALTER TABLE gmail_emails ADD CONSTRAINT email_id_not_null CHECK (email_id IS NOT NULL) NOT ENFORCED;

-- Suggested upsert key for ingestion from Gmail API: email_id
-- Suggested dedupe key within a thread: (thread_id, email_id)

-- Insert 3 dummy rows for testing
INSERT INTO dev.core.emails VALUES
(
  '18c5f2a3b4d5e6f7',                                    -- email_id
  'thread_001',                                          -- thread_id
  'Welcome to Our Service',                              -- subject
  'Support Team',                                        -- from_name
  'support@example.com',                                 -- from_email
  NULL,                                                  -- reply_to
  NULL,                                                  -- in_reply_to
  NULL,                                                  -- references
  array('user@gmail.com'),                               -- to_recipients
  NULL,                                                  -- cc_recipients
  NULL,                                                  -- bcc_recipients
  timestamp('2025-11-15 09:30:00'),                      -- sent_at
  timestamp('2025-11-15 09:30:15'),                      -- received_at
  timestamp('2025-11-15 09:30:15'),                      -- gmail_internal_date
  date('2025-11-15'),                                    -- received_date
  'Welcome to our platform! We are excited to have you on board.',  -- snippet
  'Hello,\n\nWelcome to our platform! We are excited to have you on board.\n\nBest regards,\nSupport Team',  -- body_text
  '<html><body><p>Hello,</p><p>Welcome to our platform! We are excited to have you on board.</p><p>Best regards,<br>Support Team</p></body></html>',  -- body_html
  NULL,                                                  -- raw_headers
  array('INBOX', 'UNREAD'),                              -- labels
  false,                                                 -- is_read
  false,                                                 -- is_starred
  NULL,                                                  -- importance
  false,                                                 -- spam_flag
  4523,                                                  -- message_size_bytes
  false,                                                 -- has_attachments
  NULL,                                                  -- attachments
  '12345',                                               -- gmail_history_id
  current_timestamp(),                                   -- created_at
  NULL                                                   -- updated_at
),
(
  '18c5f2a3b4d5e6f8',                                    -- email_id
  'thread_002',                                          -- thread_id
  'Your Invoice for November',                           -- subject
  'Billing Department',                                  -- from_name
  'billing@company.com',                                 -- from_email
  'billing@company.com',                                 -- reply_to
  NULL,                                                  -- in_reply_to
  NULL,                                                  -- references
  array('user@gmail.com'),                               -- to_recipients
  array('accounting@gmail.com'),                         -- cc_recipients
  NULL,                                                  -- bcc_recipients
  timestamp('2025-11-14 14:22:00'),                      -- sent_at
  timestamp('2025-11-14 14:22:10'),                      -- received_at
  timestamp('2025-11-14 14:22:10'),                      -- gmail_internal_date
  date('2025-11-14'),                                    -- received_date
  'Please find attached your invoice for November 2025.',  -- snippet
  'Dear Customer,\n\nPlease find attached your invoice for November 2025.\n\nTotal Amount: $149.99\n\nThank you for your business.\n\nBilling Department',  -- body_text
  '<html><body><p>Dear Customer,</p><p>Please find attached your invoice for November 2025.</p><p>Total Amount: $149.99</p><p>Thank you for your business.</p><p>Billing Department</p></body></html>',  -- body_html
  NULL,                                                  -- raw_headers
  array('INBOX', 'IMPORTANT'),                           -- labels
  true,                                                  -- is_read
  true,                                                  -- is_starred
  'high',                                                -- importance
  false,                                                 -- spam_flag
  8934,                                                  -- message_size_bytes
  true,                                                  -- has_attachments
  array(
    named_struct('filename', 'invoice_nov_2025.pdf', 'mime_type', 'application/pdf', 'size_bytes', 45678, 'attachment_id', 'att_001')
  ),                                                     -- attachments
  '12346',                                               -- gmail_history_id
  current_timestamp(),                                   -- created_at
  NULL                                                   -- updated_at
),
(
  '18c5f2a3b4d5e6f9',                                    -- email_id
  'thread_002',                                          -- thread_id (same thread as previous)
  'Re: Your Invoice for November',                       -- subject
  'John Doe',                                            -- from_name
  'user@gmail.com',                                      -- from_email
  NULL,                                                  -- reply_to
  '<msg-id-001@company.com>',                            -- in_reply_to
  array('<msg-id-001@company.com>'),                     -- references
  array('billing@company.com'),                          -- to_recipients
  array('accounting@gmail.com'),                         -- cc_recipients
  NULL,                                                  -- bcc_recipients
  timestamp('2025-11-14 15:45:00'),                      -- sent_at
  timestamp('2025-11-14 15:45:05'),                      -- received_at
  timestamp('2025-11-14 15:45:05'),                      -- gmail_internal_date
  date('2025-11-14'),                                    -- received_date
  'Thank you for the invoice. Payment will be processed shortly.',  -- snippet
  'Hi,\n\nThank you for the invoice. Payment will be processed shortly.\n\nBest regards,\nJohn Doe',  -- body_text
  '<html><body><p>Hi,</p><p>Thank you for the invoice. Payment will be processed shortly.</p><p>Best regards,<br>John Doe</p></body></html>',  -- body_html
  NULL,                                                  -- raw_headers
  array('SENT'),                                         -- labels
  true,                                                  -- is_read
  false,                                                 -- is_starred
  NULL,                                                  -- importance
  false,                                                 -- spam_flag
  3201,                                                  -- message_size_bytes
  false,                                                 -- has_attachments
  NULL,                                                  -- attachments
  '12347',                                               -- gmail_history_id
  current_timestamp(),                                   -- created_at
  NULL                                                   -- updated_at
);


