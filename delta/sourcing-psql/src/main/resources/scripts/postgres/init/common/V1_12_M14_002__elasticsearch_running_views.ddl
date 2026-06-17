-- Tracks, per Elasticsearch view revision, the view materialized by the coordinator.
CREATE TABLE IF NOT EXISTS public.elasticsearch_running_views(
    project       text        NOT NULL,
    view_id       text        NOT NULL,
    indexing_rev  integer     NOT NULL,
    uuid          uuid        NOT NULL,
    instant       timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY(project, view_id, indexing_rev)
);
