CREATE SCHEMA openalex;
ALTER DATABASE openalex CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;

/*differences from original noted in these comments: remove 'without time zone' (mariadb uses utc without time zone in timestamps) and set 'mag' and some boolean fields like 'is_oa' types to tinytext to avoid error attempting to load blank value as integer*/

CREATE TABLE openalex.authors (
    id text NOT NULL,
    orcid text,
    display_name text,
    display_name_alternatives json,
    works_count integer,
    cited_by_count integer,
    last_known_institution text,
    works_api_url text,
    updated_date timestamp
);

CREATE TABLE openalex.authors_counts_by_year (
    author_id text NOT NULL,
    year integer NOT NULL,
    works_count integer,
    cited_by_count integer
);

CREATE TABLE openalex.authors_ids (
    author_id text NOT NULL,
    openalex text,
    orcid text,
    scopus text,
    twitter text,
    wikipedia text,
    mag tinytext
);

/* removed 'without time zone' */

CREATE TABLE openalex.concepts (
    id text NOT NULL,
    wikidata text,
    display_name text,
    level integer,
    description text,
    works_count integer,
    cited_by_count integer,
    image_url text,
    image_thumbnail_url text,
    works_api_url text,
    updated_date timestamp
);

CREATE TABLE openalex.concepts_ancestors (
    concept_id text,
    ancestor_id text
);

CREATE TABLE openalex.concepts_counts_by_year (
    concept_id text NOT NULL,
    year integer NOT NULL,
    works_count integer,
    cited_by_count integer
);

CREATE TABLE openalex.concepts_ids (
    concept_id text NOT NULL,
    openalex text,
    wikidata text,
    wikipedia text,
    umls_aui json,
    umls_cui json,
    mag tinytext
);

CREATE TABLE openalex.concepts_related_concepts (
    concept_id text,
    related_concept_id text,
    score real
);

/* removed 'without time zone' */

CREATE TABLE openalex.institutions (
    id text NOT NULL,
    ror text,
    display_name text,
    country_code text,
    type text,
    homepage_url text,
    image_url text,
    image_thumbnail_url text,
    display_name_acroynyms json,
    display_name_alternatives json,
    works_count integer,
    cited_by_count integer,
    works_api_url text,
    updated_date timestamp
);

CREATE TABLE openalex.institutions_associated_institutions (
    institution_id text,
    associated_institution_id text,
    relationship text
);

CREATE TABLE openalex.institutions_counts_by_year (
    institution_id text NOT NULL,
    year integer NOT NULL,
    works_count integer,
    cited_by_count integer
);

CREATE TABLE openalex.institutions_geo (
    institution_id text NOT NULL,
    city text,
    geonames_city_id text,
    region text,
    country_code text,
    country text,
    latitude real,
    longitude real
);

CREATE TABLE openalex.institutions_ids (
    institution_id text NOT NULL,
    openalex text,
    ror text,
    grid text,
    wikipedia text,
    wikidata text,
    mag tinytext
);

/* removed 'without time zone' */

CREATE TABLE openalex.venues (
    id text NOT NULL,
    issn_l text,
    issn json,
    display_name text,
    publisher text,
    works_count integer,
    cited_by_count integer,
    is_oa tinytext,
    is_in_doaj tinytext,
    homepage_url text,
    works_api_url text,
    updated_date timestamp without time zone
);

CREATE TABLE openalex.venues_counts_by_year (
    venue_id text NOT NULL,
    year integer NOT NULL,
    works_count integer,
    cited_by_count integer
);

CREATE TABLE openalex.venues_ids (
    venue_id text,
    openalex text,
    issn_l text,
    issn json,
    mag tinytext
);

CREATE TABLE openalex.works (
    id text NOT NULL,
    doi text,
    title text,
    display_name text,
    publication_year integer,
    publication_date text,
    type text,
    cited_by_count integer,
    is_retracted boolean,
    is_paratext boolean,
    cited_by_api_url text,
    abstract_inverted_index json
);

CREATE TABLE openalex.works_alternate_host_venues (
    work_id text,
    venue_id text,
    url text,
    is_oa tinytext,
    version text,
    license text
);

CREATE TABLE openalex.works_authorships (
    work_id text,
    author_position text,
    author_id text,
    institution_id text,
    raw_affiliation_string text
);

CREATE TABLE openalex.works_biblio (
    work_id text NOT NULL,
    volume text,
    issue text,
    first_page text,
    last_page text
);

CREATE TABLE openalex.works_concepts (
    work_id text,
    concept_id text,
    score real
);

CREATE TABLE openalex.works_host_venues (
    work_id text,
    venue_id text,
    url text,
    is_oa tinytext,
    version text,
    license text
);

CREATE TABLE openalex.works_ids (
    work_id text NOT NULL,
    openalex text,
    doi text,
    mag tinytext,
    pmid text,
    pmcid text
);

CREATE TABLE openalex.works_mesh (
    work_id text,
    descriptor_ui text,
    descriptor_name text,
    qualifier_ui text,
    qualifier_name text,
    is_major_topic boolean
);

CREATE TABLE openalex.works_open_access (
    work_id text NOT NULL,
    is_oa tinytext,
    oa_status text,
    oa_url text
);

CREATE TABLE openalex.works_referenced_works (
    work_id text,
    referenced_work_id text
);

CREATE TABLE openalex.works_related_works (
    work_id text,
    related_work_id text
);

/* add length 255 (arbitrary) to author_id to prevent SQL error 1170 key specification without key length, adjusted syntax */
ALTER TABLE openalex.authors_counts_by_year ADD PRIMARY KEY (author_id(255), year);
ALTER TABLE openalex.authors_ids ADD PRIMARY KEY (author_id(255));
ALTER TABLE openalex.authors ADD PRIMARY KEY (id(255));
ALTER TABLE openalex.concepts_counts_by_year ADD PRIMARY KEY (concept_id(255), year);
ALTER TABLE openalex.concepts_ids ADD PRIMARY KEY (concept_id(255));
ALTER TABLE openalex.concepts ADD PRIMARY KEY (id(255));
ALTER TABLE  openalex.institutions_counts_by_year ADD PRIMARY KEY (institution_id(255), year);

ALTER TABLE openalex.institutions_geo ADD PRIMARY KEY (institution_id(255));
ALTER TABLE openalex.institutions_ids ADD PRIMARY KEY (institution_id(255));
ALTER TABLE openalex.institutions ADD PRIMARY KEY (id(255));
ALTER TABLE openalex.venues ADD PRIMARY KEY (id(255));
ALTER TABLE openalex.venues_counts_by_year ADD PRIMARY KEY (venue_id(255), year);
ALTER TABLE openalex.works_biblio ADD PRIMARY KEY (work_id(255));
ALTER TABLE openalex.works_ids ADD PRIMARY KEY (work_id(255));
ALTER TABLE openalex.works_open_access ADD PRIMARY KEY (work_id(255));
ALTER TABLE openalex.works ADD PRIMARY KEY (id(255));
CREATE INDEX concepts_ancestors_concept_id_idx ON openalex.concepts_ancestors (concept_id(255)) USING btree;
CREATE INDEX concepts_related_concepts_concept_id_idx ON openalex.concepts_related_concepts (concept_id(255)) USING btree;
CREATE INDEX concepts_related_concepts_related_concept_id_idx ON openalex.concepts_related_concepts (related_concept_id(255)) USING btree;
CREATE INDEX works_alternate_host_venues_work_id_idx ON openalex.works_alternate_host_venues (work_id(255)) USING btree;
CREATE INDEX works_host_venues_work_id_idx ON openalex.works_host_venues (work_id(255)) USING btree;
