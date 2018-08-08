from submitpipeline.cwl2workflow import add_workflow_to_insert, add_software_to_insert
basedir = "/Users/soo/git/fourfront/src/encoded/tests/data/inserts"
docker_version = 'v13'

add_software_to_insert('4dn-dcic/docker-4dn-repliseq', docker_version, basedir + '/software.json')
add_workflow_to_insert(basedir + '/workflow.json', basedir + '/software.json', 'repliseq-parta.cwl', 'repliseq', 'dev')

