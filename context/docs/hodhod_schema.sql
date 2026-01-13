-- context for ai

-- ============================================================================
-- CORE ENTITIES
-- ============================================================================

CREATE TABLE "user" (
    user_id BIGSERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    full_name VARCHAR(255),
    created_on TIMESTAMP DEFAULT NOW(),
    updated_on TIMESTAMP DEFAULT NOW(),
    deleted_at TIMESTAMP                     -- soft delete
);

-- User preferences and limits
CREATE TABLE user_settings (
    user_settings_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL UNIQUE REFERENCES "user"(user_id) ON DELETE CASCADE,

    -- Daily limits (LinkedIn rate limits to avoid account restrictions)
    daily_connection_limit INT DEFAULT 25,   -- max connection requests per day
    daily_message_limit INT DEFAULT 40,      -- max messages per day
    daily_inmail_limit INT DEFAULT 20,       -- max InMails per day

    -- Preferences
    timezone VARCHAR(50) DEFAULT 'UTC',      -- for scheduling outreach at correct local time
    auto_approve_messages BOOLEAN DEFAULT FALSE,  -- skip approval queue, send AI messages directly

    created_on TIMESTAMP DEFAULT NOW(),
    updated_on TIMESTAMP DEFAULT NOW()
);

-- ============================================================================
-- HODHOD STUDIO
-- ============================================================================

CREATE TABLE product (
    product_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES "user"(user_id) ON DELETE RESTRICT,
    name VARCHAR(255) NOT NULL,
    website_url TEXT,                   -- scraped to auto-populate product details
    description TEXT,
    value_proposition TEXT,
    target_icp TEXT,                    -- ideal customer profile description
    use_case TEXT,
    results JSONB,                      -- case studies, metrics, ROI data for social proof
    testimonials JSONB,                 -- customer quotes, logos, names for credibility
    created_on TIMESTAMP DEFAULT NOW(),
    updated_on TIMESTAMP DEFAULT NOW()
);

CREATE TABLE writing_style (
    writing_style_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES "user"(user_id) ON DELETE RESTRICT,
    name VARCHAR(255) NOT NULL,
    tone TEXT,                          -- e.g., 'professional', 'casual', 'friendly'
    example_messages TEXT[],            -- sample messages for AI to learn from
    instructions TEXT,                  -- additional prompts for AI
    created_on TIMESTAMP DEFAULT NOW(),
    updated_on TIMESTAMP DEFAULT NOW()
);

CREATE TABLE sender_profile (
    sender_profile_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES "user"(user_id) ON DELETE RESTRICT,

    name VARCHAR(255) NOT NULL,              -- "My main profile", "Sales persona"
    linkedin_username VARCHAR(255),
    headline TEXT,
    summary TEXT,                            -- about/bio
    value_props TEXT[],                      -- key talking points for AI personalization

    created_on TIMESTAMP DEFAULT NOW(),
    updated_on TIMESTAMP DEFAULT NOW()
);

-- ============================================================================
-- COMPANIES & PROSPECTS
-- ============================================================================

CREATE TABLE company (
    company_id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    linkedin_username VARCHAR(255) UNIQUE,
    website VARCHAR(255),
    industry VARCHAR(255),
    size VARCHAR(50),                   -- e.g., '51-200', '1000+'
    created_on TIMESTAMP DEFAULT NOW(),
    updated_on TIMESTAMP DEFAULT NOW()
);

-- Shared LinkedIn profile data (not user-specific)
CREATE TABLE prospect (
    prospect_id BIGSERIAL PRIMARY KEY,
    linkedin_username VARCHAR(255) NOT NULL UNIQUE,
    full_name VARCHAR(255),
    first_name VARCHAR(255),
    headline TEXT,
    location VARCHAR(255),
    linkedin_bio TEXT,
    bio_embedding VECTOR(1536),              -- embedding of: headline + bio + location (for lookalike search)
    created_on TIMESTAMP DEFAULT NOW(),
    updated_on TIMESTAMP DEFAULT NOW()
);

-- User's relationship to a prospect
CREATE TABLE user_prospect (
    user_prospect_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES "user"(user_id) ON DELETE RESTRICT,
    prospect_id BIGINT NOT NULL REFERENCES prospect(prospect_id) ON DELETE RESTRICT,

    is_contacted BOOLEAN DEFAULT FALSE,
    created_on TIMESTAMP DEFAULT NOW(),
    updated_on TIMESTAMP DEFAULT NOW(),
    deleted_at TIMESTAMP,                    -- soft delete

    UNIQUE(user_id, prospect_id)
);

-- Additional contact info (email, phone, twitter, etc.)
-- Separate table since most prospects won't have all contact types
CREATE TABLE prospect_contact (
    prospect_contact_id BIGSERIAL PRIMARY KEY,
    prospect_id BIGINT NOT NULL REFERENCES prospect(prospect_id) ON DELETE CASCADE,
    contact_type INT NOT NULL,               -- 1=email, 2=phone, 3=twitter, 4=instagram, etc.
    value VARCHAR(255) NOT NULL,
    is_primary BOOLEAN DEFAULT FALSE,
    created_on TIMESTAMP DEFAULT NOW(),
    updated_on TIMESTAMP DEFAULT NOW()
);

-- Current and past roles at companies
CREATE TABLE prospect_company (
    prospect_company_id BIGSERIAL PRIMARY KEY,
    prospect_id BIGINT NOT NULL REFERENCES prospect(prospect_id) ON DELETE CASCADE,
    company_id BIGINT NOT NULL REFERENCES company(company_id),
    role VARCHAR(255),
    started_on DATE,
    ended_on DATE,                      -- NULL if current
    is_current BOOLEAN DEFAULT FALSE,
    created_on TIMESTAMP DEFAULT NOW(),
    updated_on TIMESTAMP DEFAULT NOW(),

    UNIQUE(prospect_id, company_id, started_on),
    CHECK ((is_current = TRUE AND ended_on IS NULL) OR (is_current = FALSE)),
    CHECK (ended_on IS NULL OR ended_on >= started_on)
);

CREATE TABLE prospect_education (
    prospect_education_id BIGSERIAL PRIMARY KEY,
    prospect_id BIGINT NOT NULL REFERENCES prospect(prospect_id) ON DELETE CASCADE,
    institution VARCHAR(255),
    degree VARCHAR(255),
    field_of_study VARCHAR(255),
    started_on DATE,
    ended_on DATE,
    created_on TIMESTAMP DEFAULT NOW(),

    CHECK (ended_on IS NULL OR ended_on >= started_on)
);

-- AI research output per prospect (user-specific)
-- Stores results from AI research agents that analyze prospect data.
-- Used to personalize outreach messages and provide context for sales.
-- Examples:
--   research_type=1 (linkedin_activity): recent posts, engagement patterns, topics of interest
--   research_type=2 (company_news): funding rounds, product launches, job postings
--   research_type=3 (talking_points): AI-generated conversation starters based on research
--   research_type=4 (pain_points): inferred challenges based on role/industry
-- Multiple rows per prospect allowed (one per research type, can be refreshed)
CREATE TABLE prospect_research (
    prospect_research_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES "user"(user_id) ON DELETE RESTRICT,
    prospect_id BIGINT NOT NULL REFERENCES prospect(prospect_id) ON DELETE RESTRICT,

    research_type INT NOT NULL,
    content JSONB NOT NULL,

    researched_on TIMESTAMP DEFAULT NOW()
);

-- ICP scoring (user-specific, can be recomputed)
CREATE TABLE prospect_score (
    prospect_score_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES "user"(user_id) ON DELETE RESTRICT,
    prospect_id BIGINT NOT NULL REFERENCES prospect(prospect_id) ON DELETE RESTRICT,
    overall INT CHECK (overall BETWEEN 0 AND 100),
    problem_intensity INT CHECK (problem_intensity BETWEEN 0 AND 100),
    relevance INT CHECK (relevance BETWEEN 0 AND 100),
    likelihood_to_respond INT CHECK (likelihood_to_respond BETWEEN 0 AND 100),
    computed_on TIMESTAMP DEFAULT NOW(),
    updated_on TIMESTAMP DEFAULT NOW(),

    UNIQUE(user_id, prospect_id)
);

-- Intent signals (user-specific, changes over time)
-- level: 1=cold, 2=warm, 3=hot
CREATE TABLE prospect_intent (
    prospect_intent_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES "user"(user_id) ON DELETE RESTRICT,
    prospect_id BIGINT NOT NULL REFERENCES prospect(prospect_id) ON DELETE RESTRICT,
    level INT NOT NULL DEFAULT 1,
    signals JSONB,                           -- flexible: what triggered this level
    updated_on TIMESTAMP DEFAULT NOW(),

    UNIQUE(user_id, prospect_id)
);

-- Website activity (user-specific event log)
CREATE TABLE prospect_page_visit (
    prospect_page_visit_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES "user"(user_id) ON DELETE RESTRICT,
    prospect_id BIGINT NOT NULL REFERENCES prospect(prospect_id) ON DELETE RESTRICT,
    page_url TEXT NOT NULL,
    page_title TEXT,
    visited_at TIMESTAMP DEFAULT NOW()
);

-- ============================================================================
-- CAMPAIGNS
-- ============================================================================

-- status: 1=draft, 2=active, 3=paused, 4=completed
CREATE TABLE campaign (
    campaign_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES "user"(user_id) ON DELETE RESTRICT,
    product_id BIGINT REFERENCES product(product_id),
    sender_profile_id BIGINT REFERENCES sender_profile(sender_profile_id),
    writing_style_id BIGINT REFERENCES writing_style(writing_style_id),

    name VARCHAR(255) NOT NULL,
    status INT NOT NULL DEFAULT 1,
    source VARCHAR(50),                      -- e.g., 'website_visitors', 'linkedin_search', 'csv_import', 'sales_nav'

    created_on TIMESTAMP DEFAULT NOW(),
    updated_on TIMESTAMP DEFAULT NOW()
);

-- Campaign stats (separate table, computed or updated via triggers/app logic)
CREATE TABLE campaign_stats (
    campaign_stats_id BIGSERIAL PRIMARY KEY,
    campaign_id BIGINT NOT NULL UNIQUE REFERENCES campaign(campaign_id) ON DELETE CASCADE,
    total_prospects INT DEFAULT 0,
    total_reachouts INT DEFAULT 0,
    total_accepted INT DEFAULT 0,
    total_replied INT DEFAULT 0,
    created_on TIMESTAMP DEFAULT NOW(),
    updated_on TIMESTAMP DEFAULT NOW()
);

-- Prospects assigned to a campaign
-- status: 1=pending, 2=in_sequence, 3=completed, 4=replied, 5=opted_out
CREATE TABLE campaign_prospect (
    campaign_prospect_id BIGSERIAL PRIMARY KEY,
    campaign_id BIGINT NOT NULL REFERENCES campaign(campaign_id) ON DELETE CASCADE,
    prospect_id BIGINT NOT NULL REFERENCES prospect(prospect_id),

    status INT NOT NULL DEFAULT 1,

    created_on TIMESTAMP DEFAULT NOW(),
    updated_on TIMESTAMP DEFAULT NOW(),

    UNIQUE(campaign_id, prospect_id)
);

-- ============================================================================
-- SEQUENCES (programmable outreach steps)
-- ============================================================================

CREATE TABLE sequence (
    sequence_id BIGSERIAL PRIMARY KEY,
    campaign_id BIGINT NOT NULL REFERENCES campaign(campaign_id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,         -- user-defined name like "First Outreach", "Re-engagement"
    created_on TIMESTAMP DEFAULT NOW(),
    updated_on TIMESTAMP DEFAULT NOW()
);

CREATE TABLE sequence_step (
    sequence_step_id BIGSERIAL PRIMARY KEY,
    sequence_id BIGINT NOT NULL REFERENCES sequence(sequence_id) ON DELETE CASCADE,

    step_order INT NOT NULL,                    -- 1, 2, 3...
    step_type INT NOT NULL,                     -- app-defined: connect, message, inmail, wait, etc.

    -- Timing
    delay_days INT DEFAULT 0,                   -- days to wait before this step

    -- Content
    message_template TEXT,                      -- NULL if AI-generated
    ai_generated BOOLEAN DEFAULT TRUE,
    subject_template TEXT,                      -- for InMails/emails

    created_on TIMESTAMP DEFAULT NOW(),
    updated_on TIMESTAMP DEFAULT NOW(),

    UNIQUE(sequence_id, step_order),
    CHECK (delay_days >= 0)
);

-- ============================================================================
-- OUTREACH ACTIONS (the core activity log)
-- ============================================================================

CREATE TABLE outreach_status (
    outreach_status_id INT PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE
);

-- Seed statuses
INSERT INTO outreach_status (outreach_status_id, name) VALUES
    (1, 'scheduled'),
    (2, 'pending_approval'),
    (3, 'sent'),
    (4, 'delivered'),
    (5, 'opened'),
    (6, 'clicked'),
    (7, 'accepted'),
    (8, 'replied'),
    (9, 'interested'),
    (10, 'meeting_booked'),
    (11, 'declined'),
    (12, 'bounced'),
    (13, 'unsubscribed'),
    (14, 'no_response'),
    (15, 'not_interested'),
    (16, 'failed'),
    (17, 'rate_limited');

CREATE TABLE outreach_action (
    outreach_action_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES "user"(user_id) ON DELETE RESTRICT,
    prospect_id BIGINT NOT NULL REFERENCES prospect(prospect_id) ON DELETE RESTRICT,
    campaign_id BIGINT REFERENCES campaign(campaign_id),
    sequence_step_id BIGINT REFERENCES sequence_step(sequence_step_id),

    channel VARCHAR(50) NOT NULL CHECK (channel IN ('linkedin_message', 'linkedin_inmail', 'email')),
    action_type INT NOT NULL,                   -- app-defined
    status INT NOT NULL DEFAULT 1 REFERENCES outreach_status(outreach_status_id),

    -- Timing
    scheduled_for TIMESTAMP,
    executed_at TIMESTAMP,

    -- Email tracking
    email_message_id VARCHAR(255),
    opened_at TIMESTAMP,
    clicked_at TIMESTAMP,
    bounced_at TIMESTAMP,

    -- Response
    replied_at TIMESTAMP,

    parent_action_id BIGINT REFERENCES outreach_action(outreach_action_id),

    created_on TIMESTAMP DEFAULT NOW(),
    updated_on TIMESTAMP DEFAULT NOW()
);

-- Messages (content separate from action)
CREATE TABLE message (
    message_id BIGSERIAL PRIMARY KEY,
    outreach_action_id BIGINT NOT NULL REFERENCES outreach_action(outreach_action_id) ON DELETE CASCADE,

    subject TEXT,
    body TEXT,

    -- For replies/threads
    parent_message_id BIGINT REFERENCES message(message_id),
    direction INT NOT NULL DEFAULT 1,           -- 1=outbound, 2=inbound

    created_on TIMESTAMP DEFAULT NOW()
);

-- ============================================================================
-- INBOX / TIMELINE
-- ============================================================================

-- AI summaries for prospects (inbox overview)
CREATE TABLE prospect_summary (
    prospect_summary_id BIGSERIAL PRIMARY KEY,
    prospect_id BIGINT NOT NULL REFERENCES prospect(prospect_id) ON DELETE RESTRICT,
    user_id BIGINT NOT NULL REFERENCES "user"(user_id) ON DELETE RESTRICT,

    summary TEXT NOT NULL,
    key_points TEXT[],                          -- bullet points
    sentiment VARCHAR(20),                      -- positive, neutral, negative
    next_recommended_action TEXT,

    generated_at TIMESTAMP DEFAULT NOW(),
    valid_until TIMESTAMP,                      -- regenerate after this

    UNIQUE(prospect_id, user_id)
);

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Prospects
CREATE INDEX idx_prospect_linkedin ON prospect(linkedin_username);
CREATE INDEX idx_prospect_bio_embedding ON prospect USING ivfflat (bio_embedding vector_cosine_ops);

-- User prospects
CREATE INDEX idx_user_prospect_user ON user_prospect(user_id);
CREATE INDEX idx_user_prospect_prospect ON user_prospect(prospect_id);
CREATE INDEX idx_user_prospect_contacted ON user_prospect(user_id, is_contacted);

-- Prospect contact
CREATE INDEX idx_prospect_contact_prospect ON prospect_contact(prospect_id);
CREATE UNIQUE INDEX idx_prospect_contact_primary ON prospect_contact(prospect_id, contact_type) WHERE is_primary = TRUE;

-- Prospect research
CREATE INDEX idx_prospect_research_user_prospect ON prospect_research(user_id, prospect_id, research_type);
CREATE INDEX idx_prospect_research_content ON prospect_research USING GIN(content);

-- Prospect company
CREATE INDEX idx_prospect_company_prospect ON prospect_company(prospect_id);
CREATE INDEX idx_prospect_company_company ON prospect_company(company_id);
CREATE INDEX idx_prospect_company_current ON prospect_company(prospect_id) WHERE is_current = TRUE;

-- Prospect scores
CREATE INDEX idx_prospect_score_user ON prospect_score(user_id);
CREATE INDEX idx_prospect_score_overall ON prospect_score(user_id, overall DESC);

-- Prospect intent
CREATE INDEX idx_prospect_intent_user ON prospect_intent(user_id);
CREATE INDEX idx_prospect_intent_level ON prospect_intent(user_id, level);  -- 1=cold, 2=warm, 3=hot

-- Page visits
CREATE INDEX idx_page_visit_user_prospect ON prospect_page_visit(user_id, prospect_id, visited_at DESC);

-- Campaigns
CREATE INDEX idx_campaign_user ON campaign(user_id);
CREATE INDEX idx_campaign_status ON campaign(user_id, status);
CREATE INDEX idx_campaign_prospect_campaign ON campaign_prospect(campaign_id);
CREATE INDEX idx_campaign_prospect_prospect ON campaign_prospect(prospect_id);

-- Outreach
CREATE INDEX idx_outreach_user_status ON outreach_action(user_id, status);
CREATE INDEX idx_outreach_prospect ON outreach_action(prospect_id);
CREATE INDEX idx_outreach_scheduled ON outreach_action(scheduled_for) WHERE status = 1;  -- 1=scheduled
CREATE INDEX idx_outreach_pending ON outreach_action(user_id) WHERE status = 2;       -- 2=pending_approval
CREATE INDEX idx_outreach_parent ON outreach_action(parent_action_id) WHERE parent_action_id IS NOT NULL;

-- Messages
CREATE INDEX idx_message_outreach ON message(outreach_action_id);
CREATE INDEX idx_message_parent ON message(parent_message_id) WHERE parent_message_id IS NOT NULL;

