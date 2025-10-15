import airr
import copy
from dateutil.parser import parse

from ak_schema import (
    XSD,
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
    DataTransformation,
    AIRRGenotypeData,
    InputOutputDataMap,
)
from ak_schema_utils import (
    akc_id,
    adc_ontology,
    to_datetime,
)

''' notes from Scott:
The assay is AIRRSequencingAssay. In transform_airr_repertoire, it is created near the end. I'll note that the assay also has the repertoire_id for the ADC repertoire, 
though I don't know if this will help you because for shared studies (ADC and VDJbase) if the repertoire IDs stay the same.

Getting the participant for that assay then requires following a few links. The assay should point to a Specimen. The Specimen points to a LifeEvent (specimen collection), 
and that LifeEvent points to the Participant.

We are going to represent the Genotype as a DataSet that comes out of a DataTransformation
'''


def transform_airr_genotypes(genotype_filename, vdjbase_name_to_akc_ids, container, participant_id_to_sequencing_files):
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

    for row in data['genotype_class_list']:
        subject = row['subject_name']

        if subject not in vdjbase_name_to_akc_ids:
            print(f"Cannot find VDJbase subject name: {subject} mapped to akc participant_ids, skipping genotype")
            continue

        participant_id = vdjbase_name_to_akc_ids[subject]['participant_id']
        investigation_id = vdjbase_name_to_akc_ids[subject]['investigation_id']

        if participant_id not in participant_id_to_sequencing_files:
            print(f"Cannot find participant_id: {participant_id} mapped to akc sequencing files, skipping genotype")
            continue

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
        
        # At the moment the GenotypeSet is not used as AIRRGenotypeData takes a class list
        genotype_set = GenotypeSet(
            receptor_genotype_set_id=receptor_genotype_set_id,
            genotype_class_list=genotypes
        )

        genotype_data = AIRRGenotypeData(
            akc_id(),
            data_item_types=['genotype'],
            receptor_genotype_set_id=receptor_genotype_set_id,
            genotype_class_list=genotypes
        )

        container['datasets'][genotype_data.akc_id] = genotype_data
       
        '''
        This throws an error:
        Exception has occurred: ValueError
            DataTransformation({
            'akc_id': 'AKC:337ce816-a54c-432f-8208-47d63388e5ca',
            'data_transformation_types': [DataTransformationTypeEnum(text='genotype_inference')]
            }) is not a valid URI or CURIE
        '''
        data_transformation = DataTransformation(
            akc_id(),
            data_transformation_types=['genotype_inference'],
        )

        for recs in participant_id_to_sequencing_files[participant_id]:
            sequencing_file_id = recs['sequencing_files_id']

            # at the moment the map is not stored in the container
            # at the moment I create a separate map for each sequencing file, but perhaps the map should link to a list of sequencing files?
            io_map = InputOutputDataMap(
                data_transformation=data_transformation,
                has_specified_input=sequencing_file_id,
                has_specified_output=genotype_data.akc_id,
            )

    return container    

