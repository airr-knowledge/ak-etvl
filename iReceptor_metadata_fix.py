import airr
import os
# data import/export directories
# set ak_data_dir from the environment variable AK_DATA_DIR if it exists
ak_data_dir = os.environ.get('AK_DATA_DIR', '/ak_data')
adc_data_dir = ak_data_dir + '/vdjserver-adc-cache'
adc_cache_dir = adc_data_dir + '/cache'

##iRecepter project list with metadata issues
project_list = [
    '3860335026075537901-242ac11b-0001-012', # PRJNA381394
    '5034739262512754195-242ac11b-0001-012', # PRJNA311704-001
    '5786885556369297901-242ac11b-0001-012', # PRJNA624801
    '7094829953995379181-242ac11b-0001-012', # PRJEB1289
    '7285655350956659181-242ac11b-0001-012', # PRJNA195543
    '8434237213378080275-242ac11b-0001-012', # DOI:10.21417/AMM2022JCII
    '8498404024780320275-242ac11b-0001-012', # DOI:10.1172/JCI.insight.88242
    '7525572224111219181-242ac11b-0001-012', # PRJNA368623
    '7573504059134579181-242ac11b-0001-012', # PRJNA275625
]


for project_id in project_list:
    original_file = f'{adc_cache_dir}/{project_id}/repertoires_original.airr.json'
    new_file_name = f'{adc_cache_dir}/{project_id}/repertoires.airr.json'

    data = airr.read_airr(original_file)
    
    if project_id == '3860335026075537901-242ac11b-0001-012':
        for rep in data['Repertoire']:
            rep['subject']['sex'] = None
            rep['subject']['genotype'] = None
            rep['sample'][0]['complete_sequences'] = rep['sample'][0]['complete_sequences'].strip()
        
    elif project_id == '5034739262512754195-242ac11b-0001-012':
        for rep in data['Repertoire']:
            rep['subject']['genotype'] = None
            rep['sample'][0]['template_class'] = 'RNA'
            rep['sample'][0]['sequencing_files']['sequencing_data_id'] = None
        
    elif project_id == '5786885556369297901-242ac11b-0001-012':
        for rep in data['Repertoire']:
            keywords = rep['study'].get('keywords_study', [])
            rep['study']['keywords_study'] = ["contains_paired_chain" if kw == "has_paired_chain" else kw for kw in keywords]
            rep['subject']['genotype'] = None
            rep['sample'][0]['library_generation_method'] = rep['sample'][0]['library_generation_method'].strip()

    elif project_id == '7094829953995379181-242ac11b-0001-012':
        for rep in data['Repertoire']:
            rep['subject']['genotype'] = None
            rep['sample'][0]['library_generation_method'] = 'other'
        
    elif project_id == '7285655350956659181-242ac11b-0001-012':
        for rep in data['Repertoire']:
            rep['subject']['genotype'] = None
            rep['sample'][0]['library_generation_method'] = 'other'
            rep['subject']['sex'] = None

    
    elif project_id == '8434237213378080275-242ac11b-0001-012':
        for rep in data['Repertoire']:
            rep['subject']['genotype'] = None
            rep['sample'][0]['physical_linkage'] = "none"
    
    elif project_id == '8498404024780320275-242ac11b-0001-012':
        for rep in data['Repertoire']:
            mhc_set = rep["subject"]["genotype"].get("mhc_genotype_set", {})
            mhc_list = mhc_set.get("mhc_genotype_list", [])
            
            for entry in mhc_list:
                alleles = entry.get("mhc_alleles", [])
                for allele in alleles:
                    # Add the missing field if it doesn't exist
                    allele['reference_set_ref'] = None
            rep['sample'][0]['physical_linkage'] = "none"

        
    elif project_id == '7525572224111219181-242ac11b-0001-012':
        for rep in data['Repertoire']:
            keywords = rep['study'].get('keywords_study', [])
            rep['study']['keywords_study'] = ["contains_paired_chain" if kw == "has_paired_chain" else kw for kw in keywords]
            rep['subject']['genotype'] = None
            #remove space from pubid
            rep['study']['pub_ids'] = rep['study']['pub_ids'].replace("PMID: ", "PMID:").replace("PMID:\u00A0", "PMID:")

    elif project_id == '7573504059134579181-242ac11b-0001-012':
        for rep in data['Repertoire']:
            rep['subject']['genotype'] = None
            rep['sample'][0]['library_generation_method'] = 'other'
            
    #write the repertoire json data here
    airr.write_airr(new_file_name, data)
    print(f"Done for project: {project_id}")
        

#airr tools validation command
#airr-tools validate airr -a /ak_data/vdjserver-adc-cache/cache/project_id/repertoires.airr.json