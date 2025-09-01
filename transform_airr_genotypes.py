import airr
import copy
from dateutil.parser import parse

from ak_schema import (
    Investigation,
    Participant,
    StudyArm,
    LifeEvent,
    ImmuneExposure,
    Specimen,
    CellIsolationProcessing,
    LibraryPreparationProcessing,
    AIRRSequencingData,
    AIRRSequencingAssay,
    Reference,
    Genotype,
    GenotypeSet,
)
from ak_schema_utils import (
    akc_id,
    adc_ontology,
    to_datetime,
)


def transform_airr_genotypes(genotype_filename, container):
    """Transform ADC repertoire metadata to AK objects.
    
    Args:
        genotype_filename (str): The path to the genotype JSON file
        container (AIRRKnowledgeCommons): The container to populate
    Returns:
        AIRRKnowledgeCommons: Container with transformed data
    """
    print('Processing  genotype file:', genotype_filename)

    # Load the AIRR data
    data = airr.read_airr(genotype_filename)

    genotype_sets = []
    for row in data['genotype_class_list']:
        subject = row['subject_name']
        genotype_set = row['genotypeSet']
        receptor_genotype_set_id = genotype_set['receptor_genotype_set_id']
        class_list = genotype_set['genotype_class_list']

        genotypes = []
        for genotype in class_list:
            genotypes.append(Genotype(
                receptor_genotype_id=genotype['receptor_genotype_id'],
                locus=genotype['locus'],
                documented_alleles=copy.deepcopy(genotype.get('documented_alleles', list())),
                undocumented_alleles=copy.deepcopy(genotype.get('undocumented_alleles', list())),
                deleted_genes=copy.deepcopy(genotype.get('deleted_genes', list())),
                inference_process=genotype.get('inference_process', None)
            ))
        
        genotype_sets.append(GenotypeSet(
            akc_id(),
            receptor_genotype_set_id=receptor_genotype_set_id,
            genotype_class_list=genotypes
        ))


    return container    

