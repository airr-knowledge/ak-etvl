mkdir -p /ak_data/vdjbase/vdjbase-2025-08-231-0001-012
cd /ak_data/vdjbase/vdjbase-2025-08-231-0001-012
curl "https://vdjbase.org/api/v1/genomic/all_samples_metadata/Homo%20sapiens/IGH" >genomic_metadata_IGH.json
curl "https://vdjbase.org/api/v1/genomic/all_samples_metadata/Homo%20sapiens/IGK" >genomic_metadata_IGK.json
curl "https://vdjbase.org/api/v1/genomic/all_samples_metadata/Homo%20sapiens/IGL" >genomic_metadata_IGL.json
curl "https://vdjbase.org/api/v1/airrseq/all_samples_metadata/Homo%20sapiens/IGH" >airrseq_metadata_IGH.json
curl "https://vdjbase.org/api/v1/airrseq/all_samples_metadata/Homo%20sapiens/IGK" >airrseq_metadata_IGK.json
curl "https://vdjbase.org/api/v1/airrseq/all_samples_metadata/Homo%20sapiens/IGL" >airrseq_metadata_IGL.json
curl "https://vdjbase.org/api/v1/airrseq/all_samples_metadata/Homo%20sapiens/TRB" >airrseq_metadata_TRB.json
curl "https://vdjbase.org/api/v1/airrseq/all_subjects_genotype/Homo%20sapiens" >airrseq_all_genotypes.json
curl "https://vdjbase.org/api/v1/genomic/all_subjects_genotype/Homo%20sapiens" >genomic_all_genotypes.json
