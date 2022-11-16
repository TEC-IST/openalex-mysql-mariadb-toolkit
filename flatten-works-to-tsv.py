import csv
import glob
import gzip
import json
import os
import re

SNAPSHOT_DIR = 'openalex'
CSV_DIR = 'csv'

FILES_PER_ENTITY = int(os.environ.get('OPENALEX_DEMO_FILES_PER_ENTITY', '0'))

csv_files = {
    'institutions': {
        'institutions': {
            'name': os.path.join(CSV_DIR, 'institutions.csv.gz'),
            'columns': [
                'id', 'ror', 'display_name', 'country_code', 'type', 'homepage_url', 'image_url', 'image_thumbnail_url',
                'display_name_acroynyms', 'display_name_alternatives', 'works_count', 'cited_by_count', 'works_api_url',
                'updated_date'
            ]
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'institutions_ids.csv.gz'),
            'columns': [
                'institution_id', 'openalex', 'ror', 'grid', 'wikipedia', 'wikidata', 'mag'
            ]
        },
        'geo': {
            'name': os.path.join(CSV_DIR, 'institutions_geo.csv.gz'),
            'columns': [
                'institution_id', 'city', 'geonames_city_id', 'region', 'country_code', 'country', 'latitude',
                'longitude'
            ]
        },
        'associated_institutions': {
            'name': os.path.join(CSV_DIR, 'institutions_associated_institutions.csv.gz'),
            'columns': [
                'institution_id', 'associated_institution_id', 'relationship'
            ]
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'institutions_counts_by_year.csv.gz'),
            'columns': [
                'institution_id', 'year', 'works_count', 'cited_by_count'
            ]
        }
    },
    'authors': {
        'authors': {
            'name': os.path.join(CSV_DIR, 'authors.csv.gz'),
            'columns': [
                'id', 'orcid', 'display_name', 'display_name_alternatives', 'works_count', 'cited_by_count',
                'last_known_institution', 'works_api_url', 'updated_date'
            ]
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'authors_ids.csv.gz'),
            'columns': [
                'author_id', 'openalex', 'orcid', 'scopus', 'twitter', 'wikipedia', 'mag'
            ]
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'authors_counts_by_year.csv.gz'),
            'columns': [
                'author_id', 'year', 'works_count', 'cited_by_count'
            ]
        }
    },
    'concepts': {
        'concepts': {
            'name': os.path.join(CSV_DIR, 'concepts.csv.gz'),
            'columns': [
                'id', 'wikidata', 'display_name', 'level', 'description', 'works_count', 'cited_by_count', 'image_url',
                'image_thumbnail_url', 'works_api_url', 'updated_date'
            ]
        },
        'ancestors': {
            'name': os.path.join(CSV_DIR, 'concepts_ancestors.csv.gz'),
            'columns': ['concept_id', 'ancestor_id']
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'concepts_counts_by_year.csv.gz'),
            'columns': ['concept_id', 'year', 'works_count', 'cited_by_count']
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'concepts_ids.csv.gz'),
            'columns': ['concept_id', 'openalex', 'wikidata', 'wikipedia', 'umls_aui', 'umls_cui', 'mag']
        },
        'related_concepts': {
            'name': os.path.join(CSV_DIR, 'concepts_related_concepts.csv.gz'),
            'columns': ['concept_id', 'related_concept_id', 'score']
        }
    },
    'venues': {
        'venues': {
            'name': os.path.join(CSV_DIR, 'venues.csv.gz'),
            'columns': [
                'id', 'issn_l', 'issn', 'display_name', 'publisher', 'works_count', 'cited_by_count', 'is_oa',
                'is_in_doaj', 'homepage_url', 'works_api_url', 'updated_date'
            ]
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'venues_ids.csv.gz'),
            'columns': ['venue_id', 'openalex', 'issn_l', 'issn', 'mag']
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'venues_counts_by_year.csv.gz'),
            'columns': ['venue_id', 'year', 'works_count', 'cited_by_count']
        },
    },
    'works': {
        'works': {
            'name': os.path.join(CSV_DIR, 'works.csv.gz'),
            'columns': [
                'id', 'doi', 'title', 'display_name', 'publication_year', 'publication_date', 'type', 'cited_by_count',
                'is_retracted', 'is_paratext', 'cited_by_api_url', 'abstract_inverted_index'
            ]
        },
        'host_venues': {
            'name': os.path.join(CSV_DIR, 'works_host_venues.csv.gz'),
            'columns': [
                'work_id', 'venue_id', 'url', 'is_oa', 'version', 'license'
            ]
        },
        'alternate_host_venues': {
            'name': os.path.join(CSV_DIR, 'works_alternate_host_venues.csv.gz'),
            'columns': [
                'work_id', 'venue_id', 'url', 'is_oa', 'version', 'license'
            ]
        },
        'authorships': {
            'name': os.path.join(CSV_DIR, 'works_authorships.csv.gz'),
            'columns': [
                'work_id', 'author_position', 'author_id', 'institution_id', 'raw_affiliation_string'
            ]
        },
        'biblio': {
            'name': os.path.join(CSV_DIR, 'works_biblio.csv.gz'),
            'columns': [
                'work_id', 'volume', 'issue', 'first_page', 'last_page'
            ]
        },
        'concepts': {
            'name': os.path.join(CSV_DIR, 'works_concepts.csv.gz'),
            'columns': [
                'work_id', 'concept_id', 'score'
            ]
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'works_ids.csv.gz'),
            'columns': [
                'work_id', 'openalex', 'doi', 'mag', 'pmid', 'pmcid'
            ]
        },
        'mesh': {
            'name': os.path.join(CSV_DIR, 'works_mesh.csv.gz'),
            'columns': [
                'work_id', 'descriptor_ui', 'descriptor_name', 'qualifier_ui', 'qualifier_name', 'is_major_topic'
            ]
        },
        'open_access': {
            'name': os.path.join(CSV_DIR, 'works_open_access.csv.gz'),
            'columns': [
                'work_id', 'is_oa', 'oa_status', 'oa_url'
            ]
        },
        'referenced_works': {
            'name': os.path.join(CSV_DIR, 'works_referenced_works.csv.gz'),
            'columns': [
                'work_id', 'referenced_work_id'
            ]
        },
        'related_works': {
            'name': os.path.join(CSV_DIR, 'works_related_works.csv.gz'),
            'columns': [
                'work_id', 'related_work_id'
            ]
        },
    },
}


def flatten_works():
    file_spec = csv_files['works']

    with gzip.open(file_spec['works']['name'], 'wt', encoding='utf-8') as works_csv, \
            gzip.open(file_spec['host_venues']['name'], 'wt', encoding='utf-8') as host_venues_csv, \
            gzip.open(file_spec['alternate_host_venues']['name'], 'wt', encoding='utf-8') as alternate_host_venues_csv, \
            gzip.open(file_spec['authorships']['name'], 'wt', encoding='utf-8') as authorships_csv, \
            gzip.open(file_spec['biblio']['name'], 'wt', encoding='utf-8') as biblio_csv, \
            gzip.open(file_spec['concepts']['name'], 'wt', encoding='utf-8') as concepts_csv, \
            gzip.open(file_spec['ids']['name'], 'wt', encoding='utf-8') as ids_csv, \
            gzip.open(file_spec['mesh']['name'], 'wt', encoding='utf-8') as mesh_csv, \
            gzip.open(file_spec['open_access']['name'], 'wt', encoding='utf-8') as open_access_csv, \
            gzip.open(file_spec['referenced_works']['name'], 'wt', encoding='utf-8') as referenced_works_csv, \
            gzip.open(file_spec['related_works']['name'], 'wt', encoding='utf-8') as related_works_csv:

        works_writer = init_dict_writer(works_csv, file_spec['works'], extrasaction='ignore')
        host_venues_writer = init_dict_writer(host_venues_csv, file_spec['host_venues'])
        alternate_host_venues_writer = init_dict_writer(alternate_host_venues_csv, file_spec['alternate_host_venues'])
        authorships_writer = init_dict_writer(authorships_csv, file_spec['authorships'])
        biblio_writer = init_dict_writer(biblio_csv, file_spec['biblio'])
        #concepts_writer = init_dict_writer(concepts_csv, file_spec['concepts'])
        #ids_writer = init_dict_writer(ids_csv, file_spec['ids'], extrasaction='ignore')
        #mesh_writer = init_dict_writer(mesh_csv, file_spec['mesh'])
        ###############open_access_writer = init_dict_writer(open_access_csv, file_spec['open_access'])
        #referenced_works_writer = init_dict_writer(referenced_works_csv, file_spec['referenced_works'])
        #related_works_writer = init_dict_writer(related_works_csv, file_spec['related_works'])

        files_done = 0
        for jsonl_file_name in glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'works', '*', '*.gz')):
            print(jsonl_file_name)
            with gzip.open(jsonl_file_name, 'r') as works_jsonl:
                for work_json in works_jsonl:
                    if not work_json.strip():
                        continue

                    work = json.loads(work_json)

                    if not (work_id := work.get('id')):
                        continue

                    # works
                    if (abstract := work.get('abstract_inverted_index')) is not None:
                        work['abstract_inverted_index'] = re.sub(r'[\t\r\n]',' ',json.dumps(abstract, ensure_ascii=False))

                    works_writer.writerow(work)

                    # host_venues
                    if host_venue := (work.get('host_venue') or {}):
                        if host_venue_id := host_venue.get('id'):
                            host_venues_writer.writerow({
                                'work_id': re.sub(r'[\t\r\n]',' ',str(work_id)),
                                'venue_id': re.sub(r'[\t\r\n]',' ',str(host_venue_id)),
                                'url': re.sub(r'[\t\r\n]',' ',str(host_venue.get('url'))),
                                'is_oa': re.sub(r'[\t\r\n]',' ',str(host_venue.get('is_oa'))),
                                'version': re.sub(r'[\t\r\n]',' ',str(host_venue.get('version'))),
                                'license': re.sub(r'[\t\r\n]',' ',str(host_venue.get('license'))),
                            })

                    # alternate_host_venues
                    if alternate_host_venues := work.get('alternate_host_venues'):
                        for alternate_host_venue in alternate_host_venues:
                            if venue_id := alternate_host_venue.get('id'):
                                alternate_host_venues_writer.writerow({
                                    'work_id': re.sub(r'[\t\r\n]',' ',str(work_id)),
                                    'venue_id': re.sub(r'[\t\r\n]',' ',str(venue_id)),
                                    'url': re.sub(r'[\t\r\n]',' ',str(alternate_host_venue.get('url'))),
                                    'is_oa': re.sub(r'[\t\r\n]',' ',str(alternate_host_venue.get('is_oa'))),
                                    'version': re.sub(r'[\t\r\n]',' ',str(alternate_host_venue.get('version'))),
                                    'license': re.sub(r'[\t\r\n]',' ',str(alternate_host_venue.get('license'))),
                                })

                    # authorships
                    if authorships := work.get('authorships'):
                        for authorship in authorships:
                            if author_id := authorship.get('author', {}).get('id'):
                                institutions = authorship.get('institutions')
                                institution_ids = [i.get('id') for i in institutions]
                                institution_ids = [i for i in institution_ids if i]
                                institution_ids = institution_ids or [None]

                                for institution_id in institution_ids:
                                    authorships_writer.writerow({
                                        'work_id': re.sub(r'[\t\r\n]',' ',str(work_id)),
                                        'author_position': re.sub(r'[\t\r\n]',' ',str(authorship.get('author_position'))),
                                        'author_id': re.sub(r'[\t\r\n]',' ',str(author_id)),
                                        'institution_id': re.sub(r'[\t\r\n]',' ',str(institution_id)),
                                        'raw_affiliation_string': re.sub(r'[\t\r\n]',' ',str(authorship.get('raw_affiliation_string'))),
                                    })

                    # biblio
                    if biblio := work.get('biblio'):
                        biblio['work_id'] = re.sub(r'[\t\r\n]',' ',str(work_id))
                        biblio_writer.writerow(biblio)

                    # concepts
                    #for concept in work.get('concepts'):
                    #    if concept_id := concept.get('id'):
                    #        concepts_writer.writerow({
                    #            'work_id': work_id,
                    #            'concept_id': concept_id,
                    #            'score': concept.get('score'),
                    #        })

                    # ids
                    #if ids := work.get('ids'):
                    #    ids['work_id'] = work_id
                    #    ids_writer.writerow(ids)

                    # mesh
                    #for mesh in work.get('mesh'):
                    #    mesh['work_id'] = work_id
                    #    mesh_writer.writerow(mesh)

                    # open_access
                    #if open_access := work.get('open_access'):
                    #    open_access['work_id'] = work_id
                    #    open_access_writer.writerow(open_access)

                    # referenced_works
                    #for referenced_work in work.get('referenced_works'):
                    #    if referenced_work:
                    #        referenced_works_writer.writerow({
                    #            'work_id': work_id,
                    #            'referenced_work_id': referenced_work
                    #        })

                    # related_works
                    #for related_work in work.get('related_works'):
                    #    if related_work:
                    #        related_works_writer.writerow({
                    #            'work_id': work_id,
                    #            'related_work_id': related_work
                    #        })

            files_done += 1
            if FILES_PER_ENTITY and files_done >= FILES_PER_ENTITY:
                break


def init_dict_writer(csv_file, file_spec, **kwargs):
    writer = csv.DictWriter(
        csv_file, fieldnames=file_spec['columns'],delimiter='\t', **kwargs
    )
    writer.writeheader()
    return writer


if __name__ == '__main__':
    #flatten_concepts()
    #flatten_venues()
    #flatten_institutions()
    #flatten_authors()
    flatten_works()
