
#
# gldb.py
# Germline sets and gene description utility functions
#

import sys
import airr
import json

def loadGermline(filename):
    """Load germline sets and allele descriptions from AIRR file"""
    try:
        with open(filename, 'r', encoding='utf-8') as handle:
            data = json.load(handle)
    except:
        print("Could not read germline file: " + filename)
        raise
    print('Loaded germline database:', filename)

    # organize the data by IDs
    germline = {}
    germline['germline_sets'] = { obj['germline_set_id'] : obj for obj in data['GermlineSet'] }
    germline['allele_descriptions'] = { }
    germline['paralogs'] = { }
    germline['aliases'] = { }
    for gset in germline['germline_sets']:
        for adesc in germline['germline_sets'][gset]['allele_descriptions']:
            germline['allele_descriptions'][adesc['label']] = adesc
            if adesc['paralogs']:
                for p in adesc['paralogs']:
                    germline['paralogs'][p] = adesc
            if adesc['aliases']:
                for p in adesc['aliases']:
                    germline['aliases'][p] = adesc
    return germline

def lookupAllele(germline, allele):
    if germline['allele_descriptions'].get(allele):
        return germline['allele_descriptions'].get(allele)
    elif germline['paralogs'].get(allele):
        return germline['paralogs'].get(allele)
    elif germline['aliases'].get(allele):
        return germline['aliases'].get(allele)
    else:
        #print(f"did not find {allele} in germline")
        return None

def getSubgroup(allele_description):
    if allele_description['locus'] is None:
        print('ERROR: locus is None')
        print(allele_description)
    if allele_description['sequence_type'] is None:
        print('ERROR: sequence_type is None')
        print(allele_description)
    if allele_description['subgroup_designation'] is None:
        return allele_description['locus'] + allele_description['sequence_type']
    else:
        return allele_description['locus'] + allele_description['sequence_type'] + allele_description['subgroup_designation']

def getGene(allele_description):
    n = getSubgroup(allele_description)
    # TRG is special with no subgroup in the name because one gene per subgroup
    if allele_description['locus'] == 'TRG' and allele_description['sequence_type'] == 'V':
        n = allele_description['locus'] + allele_description['sequence_type']
    if allele_description['gene_designation'] is None:
        return n
    else:
        if allele_description['functional'] == True:
            return n + '-' + allele_description['gene_designation']
        else:
            return n + '/' + allele_description['gene_designation']

def getDisplayName(germline, allele_call, level):
    """Return display name for allele call"""
    if germline is None:
        print('ERROR: Required germline database is missing.', file=sys.stderr)
        sys.exit(1)

    # handle multiple allele calls
    fields = allele_call.split(',')
    distinct_names = []
    for field in fields:
        allele_description = germline['allele_descriptions'].get(field)
        if allele_description is None:
            print('WARNING: Germline data is missing allele description:', field, file=sys.stderr)
            continue

        segment_name = None
        if level == "allele":
            segment_name = field

        if level == "gene":
            segment_name = getGene(allele_description)

        if level == "subgroup":
            segment_name = getSubgroup(allele_description)

        if segment_name is None:
            continue

        if segment_name not in distinct_names:
            distinct_names.append(segment_name)

    return distinct_names

def transformToLevel(germline, allele_call, level):
    names = getDisplayName(germline, allele_call, level)
    return ','.join(names)
