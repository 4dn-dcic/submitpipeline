# submitpipeline
Installation
```
git clone https://github.com/4dn-dcic/submitpipeline
cd submitpipeline
pip install -e .
```

cwl draft-3 to v1.0 conversion (not tested beyond a few basic fields)
```
import cwl2workflow
cwl2workflow.cwld3_2_cwlv1(draft_3_cwl_infile, v1_cwl_outfile)
```
