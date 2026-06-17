-- Persistent log of projection terminations (completions / passivations and failures).
-- One row per (name, event_type, error_class); repeated occurrences bump the counter and update timestamps.
-- Completions store an empty string for error_class so the primary key does not need COALESCE.
CREATE TABLE IF NOT EXISTS public.projection_terminations(
    module              text            NOT NULL,
    name                text            NOT NULL,
    project             text,
    resource_id         text,
    event_type          text            NOT NULL,
    error_class         text            NOT NULL DEFAULT '',
    error_message       text            NOT NULL DEFAULT '',
    first_occurrence    timestamptz     NOT NULL,
    last_occurrence     timestamptz     NOT NULL,
    occurrences         bigint          NOT NULL DEFAULT 1,
    PRIMARY KEY(name, event_type, error_class)
);

CREATE INDEX IF NOT EXISTS projection_terminations_last_occurrence_idx
    ON public.projection_terminations(last_occurrence);
