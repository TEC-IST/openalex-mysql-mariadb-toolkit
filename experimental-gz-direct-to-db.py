#NOTE: EXPERIMENTAL! This worked when tested, but bogged down some writing authors to the database
#this could be useful if you don't have much extra storage, because it moves data directly from the compressed .csv.gz files to the database
#uses the MariaDB connector
#multithreading should be refactored
#this would likely benefit a great deal from bundling transactions with BEGIN/END to prevent excessive disk writes
#this might benefit from switching off some constraint checks in the database

import glob
import gzip
import json
import os
import mariadb
import multiprocessing
import re

conn = mariadb.connect(
user="username",
password="password",
host="localhost",
database="openalex"
)
cursor = conn.cursor()

seen_concept_ids = set()
seen_venue_ids = set()
seen_institution_ids = set()

concept_errors = set()
venue_errors = set()
institution_errors = set()
author_errors = set()
work_errors = set()

#flatten concepts
def process_concepts(file_name):
    with gzip.open(file_name, 'r') as concepts_json:
        for concept_json in concepts_json:
            #concept_json = concept_json.strip()
            if concept_json != '':
                concept = json.loads(concept_json)
                concept_id = concept.get('id')

                if concept_id !='' and concept_id not in seen_concept_ids:
                    seen_concept_ids.add(concept_id)
                    #concepts
                    try: 
                        cursor.execute("INSERT INTO concepts (id,wikidata,display_name,level,description,works_count,cited_by_count,image_url,image_thumbnail_url,works_api_url,updated_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (concept_id, concept.get('wikidata'), concept.get('display_name'), concept.get('level'), concept.get('description'), concept.get('works_count'), concept.get('cited_by_count'), concept.get('image_url'), concept.get('image_thumbnail_url'), concept.get('works_api_url'), concept.get('updated_date')))
                    except mariadb.Error as e: 
                        print(e)
                        concept_errors.add(concept_id+" "+str(e))
                    
                    #concepts_ids
                    concept_ids = concept.get('ids')
                    if concept_ids:
                        try: 
                            cursor.execute("INSERT INTO concepts_ids (concept_id,openalex,wikidata,wikipedia,umls_aui,umls_cui,mag) VALUES (?, ?, ?, ?, ?, ?, ?)", (concept_id, concept_ids.get('openalex'),concept_ids.get('wikidata'),concept_ids.get('wikipedia'),json.dumps(concept_ids.get('umls_aui'), ensure_ascii=False),json.dumps(concept_ids.get('umls_cui'), ensure_ascii=False),concept_ids.get('mag')))
                        except mariadb.Error as e: 
                            print(e)
                            concept_errors.add(concept_id+" "+str(e))
                    
                    #concepts_ancestors
                    ancestors = concept.get('ancestors')
                    if ancestors:
                        for ancestor in ancestors:
                            ancestor_id = ancestor.get('id')
                            if ancestor_id:
                                try:
                                    cursor.execute("INSERT INTO concepts_ancestors (concept_id,ancestor_id) VALUES (?, ?)", (concept_id, ancestor_id))
                                except mariadb.Error as e:
                                    print(e)
                                    concept_errors.add(concept_id+" "+str(e))
                    
                    #concepts_counts_by_year
                    counts_by_year = concept.get('counts_by_year')
                    if counts_by_year:
                        for count_by_year in counts_by_year:
                            try:
                                cursor.execute("INSERT INTO concepts_counts_by_year (concept_id,year,works_count,cited_by_count) VALUES (?, ?, ?, ?)", (concept_id, count_by_year.get('year'), count_by_year.get('works_count'), count_by_year.get('cited_by_count')))
                            except mariadb.Error as e:
                                print(e)
                                concept_errors.add(concept_id+" "+str(e))

                    #concepts_related_concepts
                    related_concepts = concept.get('related_concepts')
                    if related_concepts:
                        for related_concept in related_concepts:
                            related_concept_id = related_concept.get('id')
                            if related_concept_id:
                                try:
                                    cursor.execute("INSERT INTO concepts_related_concepts (concept_id,related_concept_id,score) VALUES (?, ?, ?)", (concept_id, related_concept_id, related_concept.get('score')))
                                except mariadb.Error as e:
                                    print(e)
                                    concept_errors.add(concept_id+" "+str(e))
    conn.commit()





def process_venues(file_name):
    with gzip.open(file_name, 'r') as venues_json:
        for venue_json in venues_json:
            if venue_json != '':
                venue = json.loads(venue_json)
                venue_id = venue.get('id')
                if venue_id != '' and venue_id not in seen_venue_ids:
                    seen_venue_ids.add(venue_id)
                    venue_issn_json = json.dumps(venue.get('issn'))
                    
                    #venues
                    try:
                        cursor.execute("INSERT INTO venues (id,issn_l,issn_json,display_name,publisher,works_count,cited_by_count,is_oa,is_in_doaj,homepage_url,works_api_url,updated_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (venue_id, venue.get('issn_l'), venue_issn_json, venue.get('display_name'), venue.get('publisher'), venue.get('works_count'), venue.get('cited_by_count'), venue.get('is_oa'), venue.get('is_in_doaj'), venue.get('homepage_url'), venue.get('works_api_url'), venue.get('updated_date')))
                    except mariadb.Error as e:
                        print(e)
                        venue_errors.add(venue_id+" "+str(e))

                    #venues_counts_by_year
                    counts_by_year = venue.get('counts_by_year')
                    if counts_by_year:
                        for count_by_year in counts_by_year:
                            try:
                                cursor.execute("INSERT INTO venues_counts_by_year (venue_id,year,works_count,cited_by_count) VALUES (?, ?, ?, ?)", (venue_id, count_by_year.get('year'), count_by_year.get('works_count'), count_by_year.get('cited_by_count')))
                            except mariadb.Error as e:
                                print(e)
                                venue_errors.add(venue_id+" "+str(e))

                    #venues_ids
                    venue_ids = venue.get('ids')
                    if venue_ids:
                        try:
                            cursor.execute("INSERT INTO venues_ids (venue_id,openalex,issn_l,issn_json,mag) VALUES (?, ?, ?, ?, ?)", (venue_id, venue_ids.get('openalex'), venue_ids.get('issn_l'), venue_issn_json, venue_ids.get('mag')))
                        except mariadb.Error as e:
                            print(e)
                            venue_errors.add(venue_id+" "+str(e))
    conn.commit()



def process_institutions(file_name):
    with gzip.open(file_name, 'r') as institutions_json:
        for institution_json in institutions_json:
            if institution_json != '':
                institution = json.loads(institution_json)
                institution_id = institution.get('id')
                if institution_id != '' and institution_id not in seen_institution_ids:
                    seen_institution_ids.add(institution_id)
                    
                    #institutions
                    try:
                        cursor.execute("INSERT INTO institutions (id,ror,display_name,country_code,type,homepage_url,image_url,image_thumbnail_url,display_name_acronyms,display_name_alternatives,works_count,works_api_url,updated_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (institution_id, institution.get('ror'), institution.get('display_name'), institution.get('country_code'), institution.get('type'), institution.get('homepage_url'), institution.get('image_url'), institution.get('image_thumbnail_url'), json.dumps(institution.get('display_name_acroynyms'), ensure_ascii=False), json.dumps(institution.get('display_name_alternatives'), ensure_ascii=False), institution.get('works_count'), institution.get('works_api_url'), institution.get('updated_date')))
                    except mariadb.Error as e:
                        print(e)
                        institution_errors.add(institution_id+" "+str(e))

                    #institutions_associated_instiutions
                    associated_institutions = institution.get('associated_institutions', institution.get('associated_insitutions'))  # typo in api
                    if associated_institutions:
                        for associated_institution in associated_institutions:
                            associated_institution_id = associated_institution.get('id')
                            if associated_institution_id:
                                try:
                                    cursor.execute("INSERT INTO institutions_associated_institutions (institution_id,associated_institution_id,relationship) VALUES (?, ?, ?)", (institution_id, associated_institution_id, associated_institution.get('relationship')))
                                except mariadb.Error as e:
                                    print(e)
                                    institution_errors.add(institution_id+" "+str(e))

                    #institutions_counts_by_year
                    counts_by_year = institution.get('counts_by_year')
                    if counts_by_year:
                        for count_by_year in counts_by_year:
                            try:
                                cursor.execute("INSERT INTO institutions_counts_by_year (institution_id,year,works_count,cited_by_count) VALUES (?, ?, ?, ?)", (institution_id, count_by_year.get('year'), count_by_year.get('works_count'), count_by_year.get('cited_by_count')))
                            except mariadb.Error as e:
                                print(e)
                                institution_errors.add(institution_id+" "+str(e))

                    #institutions_geo
                    institution_geo = institution.get('geo')
                    if institution_geo:
                        try:
                            cursor.execute("INSERT INTO institutions_geo (institution_id,city,geonames_city_id,region,country_code,country,latitude,longitude) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (institution_id, institution_geo.get('city'), institution_geo.get('geonames_city_id'), institution_geo.get('region'), institution_geo.get('country_code'), institution_geo.get('country'), institution_geo.get('latitude'), institution_geo.get('longitude')))
                        except mariadb.Error as e:
                            print(e)
                            institution_errors.add(institution_id+" "+str(e))

                    #institutions_ids
                    institution_ids = institution.get('ids')
                    if institution_ids:
                        try:
                            cursor.execute("INSERT INTO institutions_ids (institution_id,openalex,ror,grid,wikipedia,wikidata,mag) VALUES (?, ?, ?, ?, ?, ?, ?)", (institution_id, institution_ids.get('openalex'), institution_ids.get('ror'), institution_ids.get('grid'), institution_ids.get('wikipedia'), institution_ids.get('wikidata'), institution_ids.get('mag')))
                        except mariadb.Error as e:
                            print(e)
                            institution_errors.add(institution_id+" "+str(e))
    conn.commit()



def process_authors(file_name):
    with gzip.open(file_name, 'r') as authors_json:
        for author_json in authors_json:
            if author_json != '':
                author = json.loads(author_json)
                author_id = author.get('id')
                if author_id != '':

                    # authors
                    try:
                        cursor.execute("INSERT INTO authors (id,orcid,display_name,display_name_alternatives,works_count,cited_by_count,last_known_institution,works_api_url,updated_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (author_id, author.get('orcid'), author.get('display_name'), json.dumps(author.get('display_name_alternatives'), ensure_ascii=False), author.get('works_count'), author.get('cited_by_count'), (author.get('last_known_institution') or {}).get('id'), author.get('works_api_url'), author.get('updated_date')))
                    except mariadb.Error as e:
                        print(e)
                        author_errors.add(author_id+" "+str(e))

                    # ids
                    author_ids = author.get('ids')
                    if author_ids:
                        try:
                            cursor.execute("INSERT INTO authors_ids (author_id,openalex,orcid,scopus,twitter,wikipedia,mag) VALUES (?, ?, ?, ?, ?, ?, ?)", (author_id, author_ids.get('openalex'), author_ids.get('orcid'), author_ids.get('scopus'), author_ids.get('twitter'), author_ids.get('wikipedia'), author_ids.get('mag')))
                        except mariadb.Error as e:
                            print(e)
                            author_errors.add(author_id+" "+str(e))

                    # counts_by_year
                    counts_by_year = author.get('counts_by_year')
                    if counts_by_year:
                        for count_by_year in counts_by_year:
                            try:
                                cursor.execute("INSERT INTO authors_counts_by_year (author_id,year,works_count,cited_by_count) VALUES (?, ?, ?, ?)", (author_id, count_by_year.get('year'), count_by_year.get('works_count'), count_by_year.get('cited_by_count')))
                            except mariadb.Error as e:
                                print(e)
                                author_errors.add(author_id+" "+str(e))
    conn.commit()


def process_works(file_name):
    with gzip.open(file_name, 'r') as works_json:
        for work_json in works_json:
            if work_json != '':
                work = json.loads(work_json)
                work_id = work.get('id')
                if work_id != '':
                    
                    # works
                    abstract = work.get('abstract_inverted_index')
                    try:
                        cursor.execute("INSERT INTO works (id,doi,title,display_name,publication_year,publication_date,type,cited_by_count,is_retracted,is_paratext,cited_by_api_url,abstract_inverted_index) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (work_id, work.get('doi'), work.get('title'), work.get('display_name'), work.get('publication_year'), work.get('publication_date'), work.get('type'), work.get('cited_by_count'), work.get('is_retracted'), work.get('is_paratext'), work.get('cited_by_api_url'), json.dumps(abstract, ensure_ascii=False)))
                    except mariadb.Error as e:
                        print(e)
                        work_errors.add(work_id+" "+str(e))

                    # host_venues
                    host_venue = work.get('host_venue') or {}
                    if host_venue:
                        host_venue_id = host_venue.get('id')
                        if host_venue_id:
                            try:
                                cursor.execute("INSERT INTO works_host_venues (work_id,venue_id,url,is_oa,version,license) VALUES (?, ?, ?, ?, ?, ?)", (work_id, host_venue_id, host_venue.get('url'), host_venue.get('is_oa'), host_venue.get('version'), host_venue.get('license')))
                            except mariadb.Error as e:
                                print(e)
                                work_errors.add(work_id+" "+str(e))

                    # alternate_host_venues
                    alternate_host_venues = work.get('alternate_host_venues')
                    if alternate_host_venues:
                        for alternate_host_venue in alternate_host_venues:
                            venue_id = alternate_host_venue.get('id')
                            if venue_id:
                                try:
                                    cursor.execute("INSERT INTO works_alternate_host_venues (work_id,venue_id,url,url,is_oa,version,license) VALUES (?, ?, ?, ?, ?, ?, ?)", (work_id, venue_id, alternate_host_venue.get('url'), alternate_host_venue.get('url'), alternate_host_venue.get('is_oa'), alternate_host_venue.get('version'), alternate_host_venue.get('license')))
                                except mariadb.Error as e:
                                    print(e)
                                    work_errors.add(work_id+" "+str(e))

                    # authorships
                    authorships = work.get('authorships')
                    if authorships:
                        for authorship in authorships:
                            author_id = authorship.get('author', {}).get('id')
                            if author_id:
                                institutions = authorship.get('institutions')
                                institution_ids = [i.get('id') for i in institutions]
                                institution_ids = [i for i in institution_ids if i]
                                institution_ids = institution_ids or [None]

                                for institution_id in institution_ids:
                                    try:
                                        cursor.execute("INSERT INTO works_authorships (work_id,author_position,author_id,institution_id,raw_affiliation_string) VALUES (?, ?, ?, ?, ?)", (work_id, authorship.get('author_position'), author_id, institution_id, authorship.get('raw_affiliation_string')))
                                    except mariadb.Error as e:
                                        print(e)
                                        work_errors.add(work_id+" "+str(e))

                    # biblio
                    biblio = work.get('biblio')
                    if biblio:
                        try:
                            cursor.execute("INSERT INTO works_biblio (work_id,volume,issue,first_page,last_page) VALUES (?, ?, ?, ?, ?)", (work_id, biblio.get('volume'), biblio.get('issue'), biblio.get('first_page'), biblio.get('last_page')))
                        except mariadb.Error as e:
                            print(e)
                            work_errors.add(work_id+" "+str(e))

                    # concepts
                    for concept in work.get('concepts'):
                        concept_id = concept.get('id')
                        if concept_id:
                            try:
                                cursor.execute("INSERT INTO works_concepts (work_id,concept_id,score) VALUES (?, ?, ?)", (work_id, concept_id, concept.get('score')))
                            except mariadb.Error as e:
                                print(e)
                                work_errors.add(work_id+" "+str(e))

                    # ids
                    ids = work.get('ids')
                    if ids:
                        try:
                            cursor.execute("INSERT INTO works_ids (work_id,openalex,doi,mag,pmid,pmcid) VALUES (?, ?, ?, ?, ?, ?)", (work_id, ids.get('openalex'), ids.get('doi'), ids.get('mag'), ids.get('pmid'), ids.get('pmcid')))
                        except mariadb.Error as e:
                            print(e)
                            work_errors.add(work_id+" "+str(e))

                    # mesh
                    for mesh in work.get('mesh'):
                        try:
                            cursor.execute("INSERT INTO works_mesh (work_id,descriptor_ui,descriptor_name,qualifier_ui,qualifier_name,is_major_topic) VALUES (?, ?, ?, ?, ?, ?)", (work_id, mesh.get('descriptor_ui'), mesh.get('descriptor_name'), mesh.get('qualifier_ui'), mesh.get('qualifier_name'), mesh.get('is_major_topic')))
                        except mariadb.Error as e:
                            print(e)
                            work_errors.add(work_id+" "+str(e))

                    # open_access
                    open_access = work.get('open_access')
                    if open_access:
                        try:
                            cursor.execute("INSERT INTO works_open_access (work_id,is_oa,oa_status,oa_url) VALUES (?, ?, ?, ?)", (work_id, open_access.get('is_oa'), open_access.get('oa_status'), open_access.get('oa_url')))
                        except mariadb.Error as e:
                            print(e)
                            work_errors.add(work_id+" "+str(e))

                    # referenced_works
                    for referenced_work in work.get('referenced_works'):
                        if referenced_work:
                            try:
                                cursor.execute("INSERT INTO works_referenced_works (work_id,referenced_work_id) VALUES (?, ?)", (work_id, referenced_work))
                            except mariadb.Error as e:
                                print(e)
                                work_errors.add(work_id+" "+str(e))

                    # related_works
                    for related_work in work.get('related_works'):
                        if related_work:
                            try:
                                cursor.execute("INSERT INTO works_related_works (work_id,related_work_id) VALUES (?, ?)", (work_id, related_work))
                            except mariadb.Error as e:
                                print(e)
                                work_errors.add(work_id+" "+str(e))
    conn.commit()


if __name__ == '__main__':
    pool = multiprocessing.Pool(processes=64) 

    #concepts
    pool.map_async(process_concepts, glob.glob(os.path.join('data', 'concepts', '*', '*.gz')))
    concept_error_file = open('concept_errors.txt', 'w')
    concept_error_file.writelines(concept_errors)

    #venues
    pool.map_async(process_venues, glob.glob(os.path.join('data', 'venues', '*', '*.gz')))
    venue_error_file = open('venue_errors.txt', 'w')
    venue_error_file.writelines(venue_errors)
    
    pool.map_async(process_institutions, glob.glob(os.path.join('data', 'institutions', '*', '*.gz')))
    institution_error_file = open('institution_errors.txt', 'w')
    institution_error_file.writelines(institution_errors)
        
    pool.map_async(process_authors, glob.glob(os.path.join('data', 'authors', '*', '*.gz')))
    authors_error_file = open('authors_errors.txt', 'w')
    authors_error_file.writelines(author_errors)

    pool.map_async(process_works, glob.glob(os.path.join('data', 'works', '*', '*.gz')))
    work_error_file = open('work_errors.txt', 'w')
    work_error_file.writelines(work_errors)

    pool.close()
    pool.join()

    conn.close()
