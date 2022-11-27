use openalex;
LOAD DATA INFILE 'C:\\db\\openalex\\data\\institutions.tsv' IGNORE INTO TABLE institutions FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (id, ror, display_name, country_code, type, homepage_url, image_url, image_thumbnail_url, display_name_acroynyms, display_name_alternatives, works_count, cited_by_count, works_api_url, updated_date);
LOAD DATA INFILE 'C:\\db\\openalex\\data\\institutions_ids.tsv' IGNORE INTO TABLE institutions_ids FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (institution_id, openalex, ror, grid, wikipedia, wikidata, mag);
LOAD DATA INFILE 'C:\\db\\openalex\\data\\institutions_geo.tsv' IGNORE INTO TABLE institutions_geo FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (institution_id, city, geonames_city_id, region, country_code, country, latitude, longitude);
LOAD DATA INFILE 'C:\\db\\openalex\\data\\institutions_associated_institutions.tsv' IGNORE INTO TABLE institutions_associated_institutions FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (institution_id, associated_institution_id, relationship);
LOAD DATA INFILE 'C:\\db\\openalex\\data\\institutions_counts_by_year.tsv' IGNORE INTO TABLE institutions_counts_by_year FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (institution_id, year, works_count, cited_by_count);

LOAD DATA INFILE 'C:\\db\\openalex\\data\\authors.tsv' IGNORE INTO TABLE authors FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (`id`, `orcid`, `display_name`, `display_name_alternatives`, `works_count`, `cited_by_count`, `last_known_institution`, `works_api_url`, `updated_date`);
LOAD DATA INFILE 'C:\\db\\openalex\\data\\authors_ids.tsv' IGNORE INTO TABLE authors_ids FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (author_id, openalex, orcid, scopus, twitter, wikipedia, mag);
LOAD DATA INFILE 'C:\\db\\openalex\\data\\authors_counts_by_year.tsv' IGNORE INTO TABLE authors_counts_by_year FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (author_id, year, works_count, cited_by_count);

LOAD DATA INFILE 'C:\\db\\openalex\\data\\concepts.tsv' IGNORE INTO TABLE concepts FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (id, wikidata, display_name, level, description, works_count, cited_by_count, image_url, image_thumbnail_url, works_api_url, updated_date);
LOAD DATA INFILE 'C:\\db\\openalex\\data\\concepts_ancestors.tsv' IGNORE INTO TABLE concepts_ancestors FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (concept_id, ancestor_id);
LOAD DATA INFILE 'C:\\db\\openalex\\data\\concepts_counts_by_year.tsv' IGNORE INTO TABLE concepts_counts_by_year FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (concept_id, year, works_count, cited_by_count);
LOAD DATA INFILE 'C:\\db\\openalex\\data\\concepts_ids.tsv' IGNORE INTO TABLE concepts_ids FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (concept_id, openalex, wikidata, wikipedia, umls_aui, umls_cui, mag);
LOAD DATA INFILE 'C:\\db\\openalex\\data\\concepts_related_concepts.tsv' IGNORE INTO TABLE concepts_related_concepts FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (concept_id, related_concept_id, score);

LOAD DATA INFILE 'C:\\db\\openalex\\data\\venues.tsv' IGNORE INTO TABLE venues FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (id, issn_l, issn, display_name, publisher, works_count, cited_by_count, is_oa, is_in_doaj, homepage_url, works_api_url, updated_date);
LOAD DATA INFILE 'C:\\db\\openalex\\data\\venues_ids.tsv' IGNORE INTO TABLE venues_ids FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (venue_id, openalex, issn_l, issn, mag);
LOAD DATA INFILE 'C:\\db\\openalex\\data\\venues_counts_by_year.tsv' IGNORE INTO TABLE venues_counts_by_year FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (venue_id, year, works_count, cited_by_count);

LOAD DATA INFILE 'C:\\db\\openalex\\data\\works.tsv' IGNORE INTO TABLE works FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (id, doi, title, display_name, publication_year, publication_date, type, cited_by_count, is_retracted, is_paratext, cited_by_api_url, abstract_inverted_index);
LOAD DATA INFILE 'C:\\db\\openalex\\data\\works_host_venues.tsv' IGNORE INTO TABLE works_host_venues FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (work_id, venue_id, url, is_oa, version, license);
LOAD DATA INFILE 'C:\\db\\openalex\\data\\works_alternate_host_venues.tsv' IGNORE INTO TABLE works_alternate_host_venues FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (work_id, venue_id, url, is_oa, version, license);
LOAD DATA INFILE 'C:\\db\\openalex\\data\\works_authorships.tsv' IGNORE INTO TABLE works_authorships FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (work_id, author_position, author_id, institution_id, raw_affiliation_string);
LOAD DATA INFILE 'C:\\db\\openalex\\data\\works_biblio.tsv' IGNORE INTO TABLE works_biblio FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (work_id, volume, issue, first_page, last_page);
LOAD DATA INFILE 'C:\\db\\openalex\\data\\works_concepts.tsv' IGNORE INTO TABLE works_concepts FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (work_id, concept_id, score);
LOAD DATA INFILE 'C:\\db\\openalex\\data\\works_ids.tsv' IGNORE INTO TABLE works_ids FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (work_id, openalex, doi, mag, pmid, pmcid);
LOAD DATA INFILE 'C:\\db\\openalex\\data\\works_mesh.tsv' IGNORE INTO TABLE works_mesh FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (work_id, descriptor_ui, descriptor_name, qualifier_ui, qualifier_name, is_major_topic);
LOAD DATA INFILE 'C:\\db\\openalex\\data\\works_open_access.tsv' IGNORE INTO TABLE works_open_access FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (work_id, is_oa, oa_status, oa_url);
LOAD DATA INFILE 'C:\\db\\openalex\\data\\works_referenced_works.tsv' IGNORE INTO TABLE works_referenced_works FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (work_id, referenced_work_id);
LOAD DATA INFILE 'C:\\db\\openalex\\data\\works_related_works.tsv' IGNORE INTO TABLE works_related_works FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (work_id, related_work_id);
