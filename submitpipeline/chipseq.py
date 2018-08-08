from submitpipeline.cwl2workflow import add_workflow_to_insert, add_software_to_insert
basedir = "/Users/soo/git/fourfront/src/encoded/tests/data/inserts"
docker_version = 'v2'

add_software_to_insert('4dn-dcic/docker-4dn-chipseq', docker_version, basedir + '/software.json')
add_workflow_to_insert(basedir + '/workflow.json', basedir + '/software.json', 'chip-seq-alignment.cwl', 'chipseq', 'dev', maindir="cwl_awsem_v1")

