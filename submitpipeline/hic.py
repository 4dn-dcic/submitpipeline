from cwl2workflow import add_workflow_to_insert, add_software_to_insert
basedir = "/Users/soo/git/fourfront/src/encoded/tests/data/inserts" 

add_software_to_insert('4dn-dcic/docker-4dn-hic', 'v42', basedir + '/software.json')
add_workflow_to_insert(basedir + '/workflow.json', basedir + '/software.json', 'hi-c-processing-bam.cwl')

