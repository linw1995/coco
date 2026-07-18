CREATE TABLE console_graph_generation_source_revisions (
    generation BIGINT PRIMARY KEY NOT NULL,
    source_revision BIGINT NOT NULL CHECK (source_revision >= 0)
);

-- Existing generations were not published under a source revision fence. Leaving
-- them unmapped forces one safe rebuild instead of treating an unverified snapshot
-- as current.
