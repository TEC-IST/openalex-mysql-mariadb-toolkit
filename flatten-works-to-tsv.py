import glob
import gzip
import json
import os
import multiprocessing

#run this script in the openalex snapshot /data directory

def clean(input_text):
    return str(input_text).replace('\t',' ').replace('\r',' ').replace('\n',' ')

def process_concepts():
    with open('concepts.tsv', 'w', encoding="utf-8") as concept_file, open('concepts_ids.tsv', 'w', encoding="utf-8") as concept_ids_file, open('concepts_counts_by_year.tsv', 'w', encoding="utf-8") as concept_counts_by_year_file, open('concepts_ancestors.tsv', 'w', encoding="utf-8") as concept_ancestors_file, open('concepts_related_concepts.tsv', 'w', encoding="utf-8") as concept_related_concepts_file:
        for concepts_file in glob.glob(os.path.join('concepts', '*', '*.gz')):
            with gzip.open(concepts_file,'r') as concepts_json:
                for concept_json in concepts_json:
                    concept = json.loads(concept_json)
                    concept_id = clean(concept.get('id').replace('https://openalex.org/', ''))

                    if concept_id:

                        #concepts
                        concept_row = ("\t".join([concept_id,clean(concept.get('wikidata')),clean(concept.get('display_name')),clean(concept.get('level')),clean(concept.get('description')),clean(concept.get('works_count')),clean(concept.get('cited_by_count')),clean(concept.get('image_url')),clean(concept.get('image_thumbnail_url')),clean(concept.get('works_api_url')),clean(concept.get('updated_date'))]))
                        concept_file.write(concept_row + '\n')
                        
                        #concepts_ids
                        concept_ids = concept.get('ids')
                        if concept_ids:
                            concept_ids_row = ("\t".join([concept_id,clean(concept_ids.get('openalex').replace('https://openalex.org/','')),clean(concept_ids.get('wikidata')),clean(concept_ids.get('wikipedia')),clean(json.dumps(concept_ids.get('umls_aui'), ensure_ascii=False)),clean(json.dumps(concept_ids.get('umls_cui'), ensure_ascii=False)),clean(concept_ids.get('mag'))]))
                            concept_ids_file.write(str(concept_ids_row) + '\n')
                        
                        #concepts_ancestors
                        ancestors = concept.get('ancestors')
                        if ancestors:
                            for ancestor in ancestors:
                                ancestor_id = clean(ancestor.get('id').replace('https://openalex.org/', ''))
                                if ancestor_id:
                                    concepts_ancestors_row = ("\t".join([concept_id, ancestor_id]))
                                    concept_ancestors_file.write(str(concepts_ancestors_row) + '\n')
                        
                        #concepts_counts_by_year
                        counts_by_year = concept.get('counts_by_year')
                        if counts_by_year:
                            for count_by_year in counts_by_year:
                                concepts_counts_by_year_row = ("\t".join([concept_id,clean(count_by_year.get('year')),clean(count_by_year.get('works_count')),clean(count_by_year.get('cited_by_count'))]))
                                concept_counts_by_year_file.write(str(concepts_counts_by_year_row) + '\n')
                        
                        #concepts_related_concepts
                        related_concepts = concept.get('related_concepts')
                        if related_concepts:
                            for related_concept in related_concepts:
                                related_concept_id = clean(related_concept.get('id').replace('https://openalex.org/', ''))
                                if related_concept_id:
                                    concepts_related_concepts_row = ("\t".join([concept_id, related_concept_id, clean(related_concept.get('score'))]))
                                    concept_related_concepts_file.write(str(concepts_related_concepts_row) + '\n')
                        
def process_venues():
    with open('venues.tsv', 'w', encoding="utf-8") as venue_file, open('venues_ids.tsv', 'w', encoding="utf-8") as venue_ids_file, open('venues_counts_by_year.tsv', 'w', encoding="utf-8") as venue_counts_by_year_file:
        for venues_file in glob.glob(os.path.join('venues', '*', '*.gz')):
            with gzip.open(venues_file,'r') as venues_json:
                for venue_json in venues_json:
                    if venue_json:
                        venue = json.loads(venue_json)
                        venue_id = clean(venue.get('id').replace('https://openalex.org/', ''))
                        if venue_id:
                                venue_issn_json = clean(json.dumps(venue.get('issn')))
                                
                                #venues
                                venues_row = ("\t".join([venue_id,clean(venue.get('issn_l')),venue_issn_json,clean(venue.get('display_name')),clean(venue.get('publisher')),clean(venue.get('works_count')),clean(venue.get('cited_by_count')),clean(venue.get('is_oa')),clean(venue.get('is_in_doaj')),clean(venue.get('homepage_url')),clean(venue.get('works_api_url')),clean(venue.get('updated_date'))]))
                                venue_file.write(str(venues_row) + '\n')
                                venue_file.flush()

                                #venues_ids
                                venue_ids = venue.get('ids')
                                if venue_ids:
                                    venues_ids_row = ("\t".join([venue_id,clean(venue_ids.get('openalex')),clean(venue_ids.get('issn_l')),venue_issn_json,clean(venue_ids.get('mag'))]))
                                    venue_ids_file.write(str(venues_ids_row) + '\n')
                                    venue_ids_file.flush()

                                #venues_counts_by_year
                                counts_by_year = venue.get('counts_by_year')
                                if counts_by_year:
                                    for count_by_year in counts_by_year:
                                        venues_counts_by_year_row = ("\t".join([venue_id,clean(count_by_year.get('year')),clean(count_by_year.get('works_count')),clean(count_by_year.get('cited_by_count'))]))
                                        venue_counts_by_year_file.write(str(venues_counts_by_year_row) + '\n')
                                        venue_counts_by_year_file.flush()

def process_institutions():
    with open('institutions.tsv', 'w', encoding="utf-8") as institution_file, open('institutions_ids.tsv', 'w', encoding="utf-8") as institution_ids_file, open('institutions_counts_by_year.tsv', 'w', encoding="utf-8") as institution_counts_by_year_file, open('institutions_associated_institutions.tsv', 'w', encoding="utf-8") as institution_associated_institutions_file, open('institutions_geo.tsv', 'w', encoding="utf-8") as institution_geo_file:
        for institutions_file in glob.glob(os.path.join('institutions', '*', '*.gz')):
            with gzip.open(institutions_file,'r') as institutions_json:
                for institution_json in institutions_json:
                    if institution_json:
                        institution = json.loads(institution_json)
                        institution_id = clean(institution.get('id').replace('https://openalex.org/', ''))
                        if institution_id:
                            
                            #institutions
                            institutions_row=("\t".join([institution_id,clean(institution.get('ror')),clean(institution.get('display_name')),clean(institution.get('country_code')),clean(institution.get('type')),clean(institution.get('homepage_url')),clean(institution.get('image_url')),clean(institution.get('image_thumbnail_url')),clean(json.dumps(institution.get('display_name_acroynyms'), ensure_ascii=False)),clean(json.dumps(institution.get('display_name_alternatives'), ensure_ascii=False)),clean(institution.get('works_count')),clean(institution.get('cited_by_count')),clean(institution.get('works_api_url')),clean(institution.get('updated_date'))]))
                            institution_file.write(str(institutions_row) + '\n')
                            institution_file.flush()

                            #institutions_associated_instiutions
                            associated_institutions = institution.get('associated_institutions', institution.get('associated_insitutions'))  # typo in api
                            if associated_institutions:
                                for associated_institution in associated_institutions:
                                    associated_institution_id = clean(associated_institution.get('id').replace('https://openalex.org/', ''))
                                    if associated_institution_id:
                                        institutions_associated_institutions_row = ("\t".join([institution_id,associated_institution_id,clean(associated_institution.get('relationship'))]))
                                        institution_associated_institutions_file.write(str(institutions_associated_institutions_row) + '\n')
                                        institution_associated_institutions_file.flush()

                            #institutions_counts_by_year
                            counts_by_year = institution.get('counts_by_year')
                            if counts_by_year:
                                for count_by_year in counts_by_year:
                                    institutions_counts_by_year_row = ("\t".join([institution_id,clean(count_by_year.get('year')),clean(count_by_year.get('works_count')),clean(count_by_year.get('cited_by_count'))]))
                                    institution_counts_by_year_file.write(str(institutions_counts_by_year_row) + '\n')
                                    institution_counts_by_year_file.flush()

                            #institutions_geo
                            institution_geo = institution.get('geo')
                            if institution_geo:
                                institutions_geo_row = ("\t".join([institution_id,clean(institution_geo.get('city')),clean(institution_geo.get('geonames_city_id')),clean(institution_geo.get('region')),clean(institution_geo.get('country_code')),clean(institution_geo.get('country')),clean(institution_geo.get('latitude')),clean(institution_geo.get('longitude'))]))
                                institution_geo_file.write(str(institutions_geo_row) + '\n')
                                institution_geo_file.flush()

                            #institutions_ids
                            institution_ids = institution.get('ids')
                            if institution_ids:
                                institutions_ids_row = ("\t".join([institution_id, clean(institution_ids.get('openalex').replace('https://openalex.org/','')),clean(institution_ids.get('ror')),clean(institution_ids.get('grid')),clean(institution_ids.get('wikipedia')),clean(institution_ids.get('wikidata')),clean(institution_ids.get('mag'))]))
                                institution_ids_file.write(str(institutions_ids_row) + '\n')
                                institution_ids_file.flush()

def process_authors():
    with open('authors.tsv', 'w', encoding="utf-8") as author_file, open('authors_ids.tsv', 'w', encoding="utf-8") as author_ids_file, open('authors_counts_by_year.tsv', 'w', encoding="utf-8") as author_counts_by_year_file:
        for authors_file in glob.glob(os.path.join('authors', '*', '*.gz')):
            with gzip.open(authors_file,'r') as authors_json:
                for author_json in authors_json:
                    if author_json:
                        author = json.loads(author_json)
                        author_id = clean(author.get('id').replace('https://openalex.org/', ''))
                        if author_id:

                            # authors
                            author_inst_json = author.get('last_known_institution')
                            affiliation = ''
                            if author_inst_json:
                                author_inst_id = clean(author_inst_json.get('id').replace('https://openalex.org/', ''))
                                if author_inst_id:
                                    affiliation = author_inst_id
                            authors_row = ("\t".join([author_id,clean(author.get('orcid')),clean(author.get('display_name')),clean(json.dumps(author.get('display_name_alternatives'), ensure_ascii=False)),clean(author.get('works_count')),clean(author.get('cited_by_count')),affiliation,clean(author.get('works_api_url')),clean(author.get('updated_date'))]))
                            author_file.write(str(authors_row) + '\n')
                            author_file.flush()

                            # ids
                            author_ids = author.get('ids')
                            if author_ids:
                                authors_ids_row = ("\t".join([author_id,clean(author_ids.get('openalex').replace('https://openalex.org/','')),clean(author_ids.get('orcid')),clean(author_ids.get('scopus')),clean(author_ids.get('twitter')),clean(author_ids.get('wikipedia')),clean(author_ids.get('mag'))]))
                                author_ids_file.write(str(authors_ids_row) + '\n')
                                author_ids_file.flush()

                            # counts_by_year
                            counts_by_year = author.get('counts_by_year')
                            if counts_by_year:
                                for count_by_year in counts_by_year:
                                    authors_counts_by_year_row = ("\t".join([author_id,clean(count_by_year.get('year')),clean(count_by_year.get('works_count')),clean(count_by_year.get('cited_by_count'))]))
                                    author_counts_by_year_file.write(str(authors_counts_by_year_row) + '\n')
                                    author_counts_by_year_file.flush()

def process_works():
    #with open('works.tsv','w', encoding="utf-8") as work_file, open('works_host_venues.tsv','w', encoding="utf-8") as work_host_venues_file, open('works_alternate_host_venues.tsv','w', encoding="utf-8") as work_alternate_host_venues_file, open('works_authorships.tsv','w', encoding="utf-8") as work_authorships_file, open('works_biblio.tsv','w', encoding="utf-8") as work_biblio_file, open('works_concepts.tsv','w', encoding="utf-8") as work_biblio_file, open('works_concepts.tsv','w', encoding="utf-8") as work_concepts_file, open('works_ids.tsv','w', encoding="utf-8") as work_ids_file, open('works_mesh.tsv','w', encoding="utf-8") as work_mesh_file, open('works_open_access.tsv','w', encoding="utf-8") as work_open_access_file, open('works_referenced_works.tsv','w', encoding="utf-8") as work_referenced_works_file, open('works_related_works.tsv','w', encoding="utf-8") as work_related_works_file:
    with open('works_biblio.tsv','w', encoding="utf-8") as work_biblio_file:
        for works_file in glob.glob(os.path.join('works', '*', '*.gz')):
            with gzip.open(works_file,'r') as works_json:
                for work_json in works_json:
                    if work_json:
                        work = json.loads(work_json)
                        work_id = clean(work.get('id').replace('https://openalex.org/', ''))
                        if work_id:
                                                        
                            # works
                            abstract = work.get('abstract_inverted_index')
                            works_row = ("\t".join([work_id,clean(work.get('doi')),clean(work.get('title')),clean(work.get('display_name')),clean(work.get('publication_year')),clean(work.get('publication_date')),clean(work.get('type')),clean(work.get('cited_by_count')),clean(work.get('is_retracted')),clean(work.get('is_paratext')),clean(work.get('cited_by_api_url')),clean(json.dumps(abstract, ensure_ascii=False))]))
                            work_file.write(str(works_row) + '\n')
                            work_file.flush()

                            # host_venues
                            host_venue = work.get('host_venue') or {}
                            if host_venue:
                                host_venue_id = clean(host_venue.get('id'))
                                if host_venue_id:
                                    works_host_venues_row = ("\t".join([work_id,host_venue_id.replace('https://openalex.org/',''),clean(host_venue.get('url')),clean(host_venue.get('is_oa')),clean(host_venue.get('version')),clean(host_venue.get('license'))]))
                                    work_host_venues_file.write(str(works_host_venues_row) + '\n')
                                    work_host_venues_file.flush()

                            # alternate_host_venues
                            alternate_host_venues = work.get('alternate_host_venues')
                            if alternate_host_venues:
                                for alternate_host_venue in alternate_host_venues:
                                    alternate_venue_id = clean(alternate_host_venue.get('id'))
                                    if alternate_venue_id:
                                        works_alternate_host_venues_row = ("\t".join([work_id,alternate_venue_id.replace('https://openalex.org/',''),clean(alternate_host_venue.get('url')),clean(alternate_host_venue.get('url')),clean(alternate_host_venue.get('is_oa')),clean(alternate_host_venue.get('version')),clean(alternate_host_venue.get('license'))]))
                                        work_alternate_host_venues_file.write(str(works_alternate_host_venues_row) + '\n')
                                        work_alternate_host_venues_file.flush()

                            # authorships
                            authorships = work.get('authorships')
                            if authorships:
                                for authorship in authorships:
                                    author_id = authorship.get('author', {}).get('id')
                                    if author_id:
                                        institutions = authorship.get('institutions')
                                        for inst in institutions:
                                            inst_id = inst.get('id')
                                            if inst_id:
                                                works_authorships_row = ("\t".join([work_id,clean(authorship.get('author_position')),clean(author_id.replace('https://openalex.org/','')),clean(inst_id.replace('https://openalex.org/','')),clean(authorship.get('raw_affiliation_string'))]))
                                                work_authorships_file.write(str(works_authorships_row) + '\n')
                                                work_authorships_file.flush()
                            
                            # biblio
                            biblio = work.get('biblio')
                            works_biblio_row = ("\t".join([work_id,clean(biblio.get('volume')),clean(biblio.get('issue')),clean(biblio.get('first_page')),clean(biblio.get('last_page'))]))
                            work_biblio_file.write(str(works_biblio_row) + '\n')
                            work_biblio_file.flush()

                            # concepts
                            for concept in work.get('concepts'):
                                concept_id = concept.get('id')
                                if concept_id:
                                    works_concepts_row = ("\t".join([work_id,clean(concept_id.replace('https://openalex.org/','')),clean(concept.get('score'))]))
                                    work_concepts_file.write(str(works_concepts_row) + '\n')
                                    work_concepts_file.flush()

                            # ids
                            ids = work.get('ids')
                            if ids:
                                works_ids_row = ("\t".join([work_id,clean(ids.get('openalex').replace('https://openalex.org/','')),clean(ids.get('doi')),clean(ids.get('mag')),clean(ids.get('pmid')),clean(ids.get('pmcid'))]))
                                work_ids_file.write(str(works_ids_row) + '\n')
                                work_ids_file.flush()

                            # mesh
                            for mesh in work.get('mesh'):
                                works_mesh_row = ("\t".join([work_id,clean(mesh.get('descriptor_ui')),clean(mesh.get('descriptor_name')),clean(mesh.get('qualifier_ui')),clean(mesh.get('qualifier_name')),clean(mesh.get('is_major_topic'))]))
                                work_mesh_file.write(str(works_mesh_row) + '\n')
                                work_mesh_file.flush()

                            # open_access
                            open_access = work.get('open_access')
                            if open_access:
                                works_open_access_row = ("\t".join([work_id,clean(open_access.get('is_oa')),clean(open_access.get('oa_status')),clean(open_access.get('oa_url'))]))
                                work_open_access_file.write(str(works_open_access_row) + '\n')
                                work_open_access_file.flush()

                            # referenced_works
                            for referenced_work in work.get('referenced_works'):
                                if referenced_work:
                                    works_referenced_works_row = ("\t".join([work_id,clean(referenced_work.replace('https://openalex.org/',''))]))
                                    work_referenced_works_file.write(str(works_referenced_works_row) + '\n')
                                    work_referenced_works_file.flush()

                            # related_works
                            for related_work in work.get('related_works'):
                                if related_work:
                                    works_related_works_row = ("\t".join([work_id,clean(related_work.replace('https://openalex.org/',''))]))
                                    work_related_works_file.write(str(works_related_works_row) + '\n')
                                    work_related_works_file.flush()

if __name__ == "__main__":
    pool = multiprocessing.Pool(processes=5)

    c = pool.apply_async(process_concepts,())
    v = pool.apply_async(process_venues,())
    i = pool.apply_async(process_institutions,())
    a = pool.apply_async(process_authors,())
    w = pool.apply_async(process_works,())

    c.get()
    v.get()
    i.get()
    a.get()
    w.get()

    pool.close()
    pool.join()
