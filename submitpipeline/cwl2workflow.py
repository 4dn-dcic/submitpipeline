import wranglertools.fdnDCIC as fdnDCIC
from uuid import uuid4
import json
import wget


class Workflow (object):
    def __init__(self, uuid=None, steps=None, arguments=None, cwl_subdir=None):
        if uuid:
            self.uuid = uuid
        else:
            self.uuid = str(uuid4())

        self.steps = steps
        self.arguments = arguments

        self.lab = "4dn-dcic-lab"
        self.award = "1U01CA200059-01"
        self.cwl_directory_url = "https://raw.githubusercontent.com/4dn-dcic/pipelines-cwl/dev/cwl_awsem/"

        if cwl_subdir:
            self.cwl_directory_url = self.cwl_directory_url + cwl_subdir


    def as_dict(self):
        return rdict(self)


    def create_from_cwl(self, cwl, software_list=None):
        """
        create from a Cwl object
        software_list : existing list of Software objects
        """
        self.steps = [Step().create_from_cwlstep(_, cwl) for _ in cwl.steps]
        self.arguments = [Argument().create_from_cwlinput(_) for _ in cwl.inputs]
        self.arguments.extend([Argument().create_from_cwloutput(_) for _ in cwl.outputs])
        if cwl.fdn_meta:
            self.title = cwl.fdn_meta.title
            self.name = cwl.fdn_meta.name
            self.cwl_main_filename = self.name + '.cwl'
            self.app_name = self.name
            self.data_types = cwl.fdn_meta.data_types
            self.category = cwl.fdn_meta.category
            self.workflow_type = cwl.fdn_meta.workflow_type
            self.description = cwl.fdn_meta.description
        if software_list:
            self.replace_meta_software_used_with_link(software_list)
        self.add_cwl_child_names(cwl)

        return self


    def add_cwl_child_names(self, cwl):
        self.cwl_child_filenames = [_.run for _ in cwl.steps]


    def replace_meta_software_used_with_link(self, software_list):
        """
        replace software name in software_used to uuid link
        """
        self.steps = [_.replace_meta_software_used_with_link(software_list) for _ in self.steps]


class Step (object):
    def __init__(self, name=None, meta=None, inputs=None, outputs=None):
        self.name = name
        self.meta = meta
        self.inputs = inputs
        self.outputs = outputs


    def as_dict(self):
        return rdict(self)


    def create_from_cwlstep(self, cwlstep, cwl):
        """
        cwlstep: a CwlStep object (a single step element)
        cwl: a Cwl object (for inputs, outputs, sourcetarget_list)
        """
        self.name = cwlstep.name

        # step inputs
        if hasattr(cwlstep, 'inputs'):  # cwl draft-3
            self.inputs = [StepInput().create_from_cwlstepinput(_, cwl) for _ in cwlstep.inputs]
        elif hasattr(cwlstep, '__in'):  # cwl v1.0
            self.inputs = [StepInput().create_from_cwlstepinput(_, cwl) for _ in cwlstep.__in]

        # step outputs
        if hasattr(cwlstep, 'outputs'):  # cwl draft-3
            self.outputs = [StepOutput().create_from_cwlstepoutput(self.name, _, cwl) for _ in cwlstep.outputs]
        elif hasattr(cwlstep, 'out'):  # cwl v1.0
            self.outputs = [StepOutput().create_from_cwlstepoutput(self.name, _, cwl) for _ in cwlstep.out]
        if cwlstep.fdn_step_meta:
            self.meta = cwlstep.fdn_step_meta.as_dict()
        return self


    def replace_meta_software_used_with_link(self, software_list=None):
        """
        replace software title (name_version or name_commit) in software_used to uuid link
        software_list is a list of Software objects
        """
        sf_uuid={}
        for sf in software_list:
            sf_uuid[sf.title] = sf.uuid

        if 'software_used' in self.meta:
            for i, sf in enumerate(self.meta.get('software_used')):
                try:
                    sf_key = self.meta['software_used'][i]  # title
                    if sf_key in sf_uuid:
                        sw_uuid = sf_uuid.get(sf_key)
                        self.meta['software_used'][i] = '/software/' + sw_uuid + '/'
                    else:
                        raise KeyError(sf_key)
                except:
                    print("Cannot replace software name with uuid link: {}".format(sf))
                    raise
        return self


class Argument (object):
    def __init__(self, workflow_argument_name=None,
                 argument_type=None,  # "Input file", "parameter", "Output processed file",
                                      # "Output QC file" or "Output report file"
                 argument_format=None,
                 secondary_file_formats=None):
        self.workflow_argument_name = workflow_argument_name
        self.argument_type = argument_type
        if argument_format:
            self.argument_format = argument_format
        if secondary_file_formats:
            self.secondary_file_formats = secondary_file_formats


    def as_dict(self):
        return rdict(self)


    def create_from_cwloutput(self, cwl_output):
        """
        convert from a CwlOutput object
        """
        self.workflow_argument_name = cwl_output.name
        if cwl_output.fdn_format:
            self.argument_format = cwl_output.fdn_format
        if cwl_output.fdn_output_type:
            self.argument_type = "Output " + cwl_output.fdn_output_type + " file"
        if cwl_output.fdn_secondary_file_formats:
            self.secondary_file_formats = cwl_output.fdn_secondary_file_formats
        return self


    def create_from_cwlinput(self, cwl_input):
        """
        convert from a CwlInput object
        """
        self.workflow_argument_name = cwl_input.name
        if cwl_input.isFile:
            self.argument_type = "Input file"
        else:
            self.argument_type = "parameter"
        if cwl_input.fdn_format:
            self.argument_format = cwl_input.fdn_format
        if cwl_input.fdn_secondary_file_formats:
            self.secondary_file_formats = cwl_input.fdn_secondary_file_formats
        return self


class StepInput (object):
    def __init__(self, name=None,
                 source_name=None,
                 source_step=None):

        self.name = name
        self.meta = {"global": False}  # default global false

        if source_name:
            self.source = [{ "name": source_name }]
            if source_step:
                self.source[0].append({"step": source_step})
            else:
                self.meta['global'] = True

    def as_dict(self):
        return rdict(self)


    def create_from_cwlstepinput(self, cwl_stepinput, cwl):
        """
        convert from a CwlStepInput object
        cwl_stepinput: a CwlStepInput object (a single step input element)
        cwl: a Cwl object (for inputs, outputs, sourcetarget_list)
        """
        # add name
        self.name = cwl_stepinput.arg_name

        # add source
        if cwl_stepinput.source:
            if cwl_stepinput.source_step:
                self.source = [{ "name": cwl_stepinput.source_arg,
                                 "step": cwl_stepinput.source_step }]
            else:
                self.source = [{ "name": cwl_stepinput.source_arg }]
                self.meta['global'] = True

        # add format (from global arguments)
        # self.add_fdn_tags_from_cwl(cwl.inputs)

        # individual format
        if cwl_stepinput.fdn_format:
            self.meta['file_format'] = cwl_stepinput.fdn_format
        if cwl_stepinput.fdn_type:
            self.meta['type'] = cwl_stepinput.fdn_type
        if cwl_stepinput.fdn_cardinality:
            self.meta['cardinality'] = cwl_stepinput.fdn_cardinality

        return self


    def add_fdn_tags_from_cwl(self, cwl_inputs):
        """
        add fdn_format info for the step inputs that source to global input
        assumes self.source has already been filled in.
        """
        for tg in self.source:
            if 'step' not in tg:
                self.meta['global'] = True
                workflow_arg_name = tg.get('name')
                break

        if self.meta['global']:
            for op in cwl_inputs:
                if op.name == workflow_arg_name:
                    self.meta['file_format'] = op.fdn_format
                    self.meta['type'] = cwl_stepinput.fdn_type
                    self.meta['cardinality'] = cwl_stepinput.fdn_cardinality
                    break


class StepOutput (object):
    def __init__(self, name=None, 
                 target_name=None,
                 target_step=None):

        self.name = name
        self.target = None

        self.meta = {"global": False}

        if target_name:
            self.target = [{ "name": target_name }]
            if target_step:
                self.target[0].append({"step": target_step})
            else:
                self.meta['global'] = True


    def as_dict(self):
        return rdict(self)


    def create_from_cwlstepoutput(self, parent_stepname, cwl_stepoutput, cwl):
        """
        convert from CwlStepOutput object
        parent_stepname : name of the step where the step output belongs to
        cwl_stepoutput : a CwlStepOutput object ( a single step output element)
        cwl : a Cwl object
        """
        self.name = cwl_stepoutput.arg_name
        self.add_target_from_cwloutputs(parent_stepname, cwl.sourcetarget_list)

        # format (from global arguments)
        # self.add_fdn_tags_from_cwl(cwl.outputs)

        # individual format
        if cwl_stepoutput.fdn_format:
            self.meta['file_format'] = cwl_stepoutput.fdn_format
        if cwl_stepoutput.fdn_type:
            self.meta['type'] = cwl_stepoutput.fdn_type
        if cwl_stepoutput.fdn_cardinality:
            self.meta['cardinality'] = cwl_stepoutput.fdn_cardinality

        return self


    def add_target_from_cwloutputs(self, parent_stepname, cwl_sourcetarget_list):
        """
        add target info from SourceTarget object
        """
        for st in cwl_sourcetarget_list:
            if st.source_step == parent_stepname and st.source_arg == self.name:
                if not hasattr(self, 'target') or not self.target:
                    self.target = []
                if st.target_step:
                    self.target.append({"name": st.target_arg,
                                        "step": st.target_step})
                else:
                    self.target.append({"name": st.target_arg})
                    self.meta['global'] = True


    def add_fdn_tags_from_cwl(self, cwl_outputs):
        """
        add fdn_format info for the step outputs that target to global output
        assumes self.target has already been filled in.
        """
        if self.target:
            for tg in self.target:
                if 'step' not in tg:
                    self.meta['global'] = True
                    workflow_arg_name = tg.get('name')
                    break

        if self.meta['global']:
            for op in cwl_outputs:
                if op.name == workflow_arg_name:
                    self.meta['file_format'] = op.fdn_format
                    self.meta['type'] = op.fdn_type
                    self.meta['cardinality'] = op.fdn_cardinality
                    break


    def add_fdn_output_type_from_cwl(self, cwl_outputs):
        """
        add fdn_output_type info for the step outputs that target to global output
        assumes self.target has already been filled in.
        """
        if self.target:
            for tg in self.target:
                if not tg.get('step'):
                    self.meta['global'] = True
                    workflow_arg_name = tg.get('name')
                    break

        if self.meta['global']:
            for op in cwl_outputs:
                if op.name == workflow_arg_name:
                    self.meta['argument_type'] = op.fdn_output_type
                    break


class CwlOutput (object):
    def __init__(self, id=None, outputSource=None, doc=None,
                 fdn_format=None, fdn_output_type=None,  # 4dn-specific custom fields
                 fdn_secondary_file_formats=None,  # 4dn-specific custom field
                 **kwargs):  # for type
        """
        take in elements of a cwl's 'outputs' field (dictionary) as kwargs
        """
        assert(id)

        # id, name
        self.id = id
        self.name = id.strip('#')

        # type (as in cwl), isFile, isArray (parsed)
        self._type = kwargs.get("type")
        self.isFile = False
        self.isArray = False
        if self._type:
            if "File" in self._type:
                self.isFile = True
            else:
                for t in self._type:
                    if isinstance(t, dict) and 'type' in t and 'items' in t:
                        if t.get('type') == 'array':
                            self.isArray = True
                            if t.get('items') == 'File':
                                self.isFile = True

        # source (as in cwl), source_step, source_arg (parsed)
        if outputSource:
            self.outputSource = outputSource
            self.source_step, self.source_arg = self.source.strip('#').split('/')

        self.fdn_format = fdn_format
        self.doc = doc
        self.fdn_output_type = fdn_output_type
        self.fdn_secondary_file_formats = fdn_secondary_file_formats


class CwlOutputD3 (object):
    def __init__(self, id=None, source=None, description=None,
                 fdn_format=None, fdn_output_type=None,  # 4dn-specific custom fields
                 fdn_secondary_file_formats=None,  # 4dn-specific custom field
                 **kwargs):  # for type
        """
        take in elements of a cwl's 'outputs' field (dictionary) as kwargs
        """
        assert(id)

        # id, name
        self.id = id
        self.name = id.strip('#')

        # type (as in cwl), isFile, isArray (parsed)
        self._type = kwargs.get("type")
        self.isFile = False
        self.isArray = False
        if self._type:
            if "File" in self._type:
                self.isFile = True
            else:
                for t in self._type:
                    if isinstance(t, dict) and 'type' in t and 'items' in t:
                        if t.get('type') == 'array':
                            self.isArray = True
                            if t.get('items') == 'File':
                                self.isFile = True

        # source (as in cwl), source_step, source_arg (parsed)
        if source:
            self.source = source
            self.source_step, self.source_arg = self.source.strip('#').split('.')

        self.fdn_format = fdn_format
        self.description = description
        self.fdn_output_type = fdn_output_type
        self.fdn_secondary_file_formats = fdn_secondary_file_formats


    def get_sourcetarget(self):
        return SourceTarget(self.source_step, self.source_arg, None, self.name)


class CwlInput (object):
    def __init__(self, id=None, default=None, doc=None,
                 fdn_format=None, fdn_secondary_file_formats=None,  # 4dn-specific custom fields
                 **kwargs):  # including id and type
        """
        take in elements of a cwl's 'inputs' field (dictionary) as kwargs
        """

        # id (as in cwl), name (parsed)
        self.id = id
        if id:
            self.name = id.strip('#')

        # type (as in cwl), isFile, isArray (parsed)
        self._type = kwargs.get("type")
        self.isFile = False
        self.isArray = False
        if self._type:
            if "File" in self._type:
                self.isFile = True
            else:
                for t in self._type:
                    if isinstance(t, dict) and 'type' in t and 'items' in t:
                        if t.get('type') == 'array':
                            self.isArray = True
                            if t.get('items') == 'File':
                                self.isFile = True

        self.default = default
        self.doc = doc
        self.fdn_format = fdn_format
        self.fdn_secondary_file_formats = fdn_secondary_file_formats


class CwlInputD3 (object):
    def __init__(self, id=None, default=None, description=None,
                 fdn_format=None, fdn_secondary_file_formats=None,  # 4dn-specific custom fields
                 **kwargs):  # including id and type
        """
        take in elements of a cwl's 'inputs' field (dictionary) as kwargs
        """

        # id (as in cwl), name (parsed)
        self.id = id
        if id:
            self.name = id.strip('#')

        # type (as in cwl), isFile, isArray (parsed)
        self._type = kwargs.get("type")
        self.isFile = False
        self.isArray = False
        if self._type:
            if "File" in self._type:
                self.isFile = True
            else:
                for t in self._type:
                    if isinstance(t, dict) and 'type' in t and 'items' in t:
                        if t.get('type') == 'array':
                            self.isArray = True
                            if t.get('items') == 'File':
                                self.isFile = True

        self.default = default
        self.description = description
        self.fdn_format = fdn_format
        self.fdn_secondary_file_formats = fdn_secondary_file_formats


class CwlStepOutput (object):
    def __init__(self, id=None, fdn_format=None, fdn_type=None, fdn_cardinality=None):
        """
        take in elements of a cwl's 'steps::outputs' field (dictionary) as kwargs
        """
        assert(id)

        # id (as in cwl), name, step_name, arg_name (parsed)
        self.id = id
        self.name = id.strip('#')
        self.step_name, self.arg_name = self.name.split('/')

        # 4dn tags
        self.fdn_format = fdn_format
        self.fdn_type = fdn_type
        self.fdn_cardinality = fdn_cardinality


class CwlStepOutputD3 (object):
    def __init__(self, id=None, fdn_format=None, fdn_type=None, fdn_cardinality=None):
        """
        take in elements of a cwl's 'steps::outputs' field (dictionary) as kwargs
        """
        assert(id)

        # id (as in cwl), name, step_name, arg_name (parsed)
        self.id = id
        self.name = id.strip('#')
        self.step_name, self.arg_name = self.name.split('.')

        # 4dn tags
        self.fdn_format = fdn_format
        self.fdn_type = fdn_type
        self.fdn_cardinality = fdn_cardinality


class CwlStepInput (object):
    def __init__(self, id=None, source=None, fdn_format=None, fdn_type=None, fdn_cardinality=None):
        """
        take in elements of a cwl's 'steps::inputs' field (dictionary) as kwargs
        """
        assert(id)

        # id (as in cwl), name, step_name, arg_name (parsed)
        self.id = id
        self.name = id.strip('#')
        self.step_name, self.arg_name = self.name.split('/')

        # source (as in cwl), source_step, source_arg (parsed)
        if source:
            self.source = source
            if len(self.source.strip('#').split('/')) == 2:
                self.source_step, self.source_arg = self.source.strip('#').split('/')
            else:
                self.source_arg = self.source.strip('#')
                self.source_step = None
        else:
            self.source = None
            self.source_step = None
            self.source_arg = None

        # 4dn tags
        self.fdn_format = fdn_format
        self.fdn_type = fdn_type
        self.fdn_cardinality = fdn_cardinality

    def get_sourcetarget(self):
        return SourceTarget(self.source_step, self.source_arg, self.step_name, self.arg_name)


class CwlStepInputD3 (object):
    def __init__(self, id=None, source=None, fdn_format=None, fdn_type=None, fdn_cardinality=None):
        """
        take in elements of a cwl's 'steps::inputs' field (dictionary) as kwargs
        """
        assert(id)

        # id (as in cwl), name, step_name, arg_name (parsed)
        self.id = id
        self.name = id.strip('#')
        self.step_name, self.arg_name = self.name.split('.')

        # source (as in cwl), source_step, source_arg (parsed)
        if source:
            self.source = source
            if len(self.source.strip('#').split('.')) == 2:
                self.source_step, self.source_arg = self.source.strip('#').split('.')
            else:
                self.source_arg = self.source.strip('#')
                self.source_step = None
        else:
            self.source = None
            self.source_step = None
            self.source_arg = None

        # 4dn tags
        self.fdn_format = fdn_format
        self.fdn_type = fdn_type
        self.fdn_cardinality = fdn_cardinality

    def get_sourcetarget(self):
        return SourceTarget(self.source_step, self.source_arg, self.step_name, self.arg_name)


class CwlStep (object):
    def __init__(self, id=None, run=None, out=None,
                 scatter=None,
                 fdn_step_meta=None,  # 4dn-specific custom tag
                 **kwargs): # kwargs includes field 'in'
        """
        take in elements of a cwl's 'steps' field (dictionary) as kwargs
        """
        self.id = id
        if id:
            self.name = id.strip('#')
        self.run = run
        self.out = [CwlStepOutput(**_) for _ in out]
        self.__in = [CwlStepInput(**_) for _ in kwargs.get('in')]
        self.fdn_step_meta = CwlFdnStepMeta(**fdn_step_meta)
        self.scatter = scatter


class CwlStepD3 (object):
    def __init__(self, id=None, run=None, outputs=None, inputs=None,
                 scatter=None,
                 fdn_step_meta=None):  # 4dn-specific custom tag
        """
        take in elements of a cwl's 'steps' field (dictionary) as kwargs
        """
        self.id = id
        if id:
            self.name = id.strip('#')
        self.run = run
        self.outputs = [CwlStepOutputD3(**_) for _ in outputs]
        self.inputs = [CwlStepInputD3(**_) for _ in inputs]
        if fdn_step_meta:
            self.fdn_step_meta = CwlFdnStepMeta(**fdn_step_meta)
        self.scatter = scatter


class Cwl (object):
     def __init__(self, inputs=None, outputs=None, steps=None,
                  cwlVersion=None, requirements=None,
                  fdn_meta=None,  # 4dn-specific custom tag
                  **kwargs):  # kwargs includes field 'class'
        """
        take in a cwl as kwargs
        """
        self._class = kwargs.get('class')
        self.cwlVersion = cwlVersion

        assert(self._class) == 'Workflow'
        assert(self.cwlVersion) == 'draft-3'

        if self.cwlVersion == 'draft-3':
            self.inputs = [CwlInputD3(**_) for _ in inputs]
            self.outputs = [CwlOutputD3(**_) for _ in outputs]
            self.steps = [CwlStepD3(**_) for _ in steps]
        elif self.cwlVersion == 'v1.0':
            self.inputs = [CwlInput(**_) for _ in inputs]
            self.outputs = [CwlOutput(**_) for _ in outputs]
            self.steps = [CwlStep(**_) for _ in steps]

        self.requirements = requirements
        if fdn_meta:
          self.fdn_meta = CwlFdnMeta(**fdn_meta)
        else:
          self.fdn_meta = None

        # list of all SourceTarget objects in CWL
        self.sourcetarget_list = [_.get_sourcetarget() for _ in self.outputs]
        for step in self.steps:
            self.sourcetarget_list.extend([_.get_sourcetarget() for _ in step.inputs])

 
class CwlFdnMeta (object):
    def __init__(self, title=None, name=None, data_types=None, category=None,
                 workflow_type=None, description=None):
        """
        take in cwl's 'fdn_meta' field (dictionary) as kwargs
        """
        self.title = title
        self.name = name
        self.data_types = data_types
        self.category = category
        self.workflow_type = workflow_type
        self.description = description


class CwlFdnStepMeta (object):
    def __init__(self, software_used=None, description=None, analysis_step_types=None):
        """
        take in cwl's 'steps::fdn_step_meta' field (dictionary) as kwargs
        """
        self.software_used = software_used
        self.description = description
        self.analysis_step_types = analysis_step_types


    def as_dict(self):
        return rdict(self)


class SourceTarget (object):
    def __init__(self, source_step=None, source_arg=None, target_step=None, target_arg=None):
        self.source_step = source_step
        self.source_arg = source_arg
        self.target_step = target_step
        self.target_arg = target_arg


    def as_dict(self):
        return rdict(self)


def create_cwl_from_file(file):
    with open(file, 'r') as f:
        cwldict = json.load(f)
    try:
        return Cwl(**cwldict)
    except AssertionError as e:
        print e
        return None

def rdict(x):
    """
    recursive conversion to dictionary
    converts objects in list members to dictionary recursively
    attributes beginning with '_' is converted to fields excluding the '_'
    """
    if isinstance(x, list):
        l = [rdict(_) for _ in x]
        return l
    elif isinstance(x, dict):
        x2={}
        for k, v in x.items():
            if k.startswith('_'):
                k2 = k[1:]
                x2[k2] = rdict(v)
            else:
                x2[k] = rdict(v)
        return x2
    else:
        if hasattr(x, '__dict__'):
            d = x.__dict__
            toremove=[]

            # convert _key -> key
            for k, v in d.items():
                if k.startswith('_'):
                    k2 = k[1:]
                    d[k2] = v
                    toremove.append(k)

            # remove items with a None value
            for k, v in d.items():
                if v is None:
                    toremove.append(k)
            for k in toremove:
                del(d[k])

            # go deep
            for k, v in d.items():
                d[k] = rdict(v)
            return d
        else:
            return x


class Software(object):

    def __init__(self, uuid=None, name=None, version=None, commit=None,
                 software_type=None, title=None, source_url=None, 
                 description=None, documentation_url=None, purpose=None, references=None,
                 **kwargs):
        self.uuid = None
        self.name = None
        self.version = None
        self.commit = None
        self.title = None
        self.source_url = None
        self.description = None
        self.documentation_url = None
        self.purpose = None
        self.references = None

        self.add(uuid, name, version, commit, software_type, title, source_url,
                 description, documentation_url, purpose, references)
        self.award = "1U01CA200059-01"
        self.lab = "4dn-dcic-lab"


    def add(self, uuid=None, name=None, version=None, commit=None,
            software_type=None, title=None, source_url=None,
            description=None, documentation_url=None, purpose=None, references=None):

        if uuid:
            self.uuid = uuid
        if name:
            self.name = name
        if version:
            self.version = version
        if commit:
            self.commit = commit
        if source_url:
            self.source_url = source_url

        if software_type:
            if not isinstance(software_type, list):
                software_type = software_type.split(',')
            self.software_type = software_type

        if title:
            self.title = title
        elif self.name:
            if self.version:
              self.title = self.name + '_' + self.version
            elif self.commit:
              self.title = self.name + '_' + self.commit[:6]
            else:
              self.title = self.name

        if not self.uuid and not uuid:
            self.assign_uuid()
        if description:
            self.description = description
        if documentation_url:
            self.documentation_url = documentation_url
        if purpose:
            self.purpose = purpose
        if references:
            self.references = references

    def as_dict(self):
        return rdict(self)


    def comp_core(self, name, version=None, commit=None):
        """
        return true if the current object is identical to the name, version and commit provided
        """
        if self.name != name:
            return False
        if self.version != version:
            return False
        if self.commit != commit:
            return False
        return True


    def comp(self, software):
        """
        return true if the current object is identical to another software object provided
        """
        return self.comp_core(software.name, software.version, software.commit)


    def assign_uuid(self):
        """
        assign random uuid
        """
        self.uuid = str(uuid4())


def map_field(field):
    """
    converts field name in text (e.g. 'SOFTWARE') to field name in class Software (e.g. 'name')
    """
    field_map = { 'SOFTWARE': 'name',
                  'VERSION': 'version',
                  'COMMIT': 'commit',
                  'TYPE': 'software_type',
                  'SOURCE_URL': 'source_url' }
    if field not in field_map:
        print("field{} not in field_map.".format(field))

    return field_map.get(field)


def parser(textfile):
    """
    returns a list of Software objects parsed from a text file containing fields
    in header lines starting with '##'.
    The software objects are in dictionary.
    """
    swlist=[]
    startover=False
    with open(textfile, 'r') as f:
        for x in f:
            x = x.strip('\n')
            if not x:
                startover = True
            else:
                if startover:
                    swlist.append(Software())
                startover = False
    
                if x.startswith('##'):
                    field, value = x.strip('## ').split(': ')[0:2]
                    field = field.strip(':')
                    swlist[-1].add(**{map_field(field): value})

    return swlist


def get_existing(insert_jsonfile):
    """
    returns a list of Software objects parsed from an insert json file
    The software objects are in dictionary
    """
    with open(insert_jsonfile, 'r') as f:
        d = json.load(f)
    swlist = [Software(**_) for _ in d]
    return [_.as_dict() for _ in swlist]


def filter_swlist(sl, sl_exist):
    """
    filters out from a swlist (sl) elements that exist in sl_exist
    """
    swlist = [Software(**_) for _ in sl]
    swlist_exist = [Software(**_) for _ in sl_exist]
    sl_new = []
    for sw in swlist:
        remove=False
        for sw2 in swlist_exist:
            if sw.comp(sw2):
                remove=True
        if not remove:
            sl_new.append(sw.as_dict())
    return(sl_new)


def add_software_to_insert(docker_reponame, docker_version, insert_jsonfile, post=False, keypairs_file=None):
    """
    downloads a text file from a docker repo that contains software info and adds them to insert_jsonfile
    """
    # download downloads.sh file from a docker repo
    downloaded_file_name = download_dockerinfo(docker_reponame, docker_version)

    # get a filtered list of software
    sl1o = parser(downloaded_file_name)
    sl1 = [_.as_dict() for _ in sl1o]

    sl2 = get_existing(insert_jsonfile)
    sl_filtered = filter_swlist(sl1, sl2)
    sl2.extend(sl_filtered)

    # overwrite insert_jsonfile
    with open(insert_jsonfile, 'w') as fw:
        json.dump(sl2, fw, sort_keys=True, indent=4)

    #post to 4dn data portal
    if post and keypairs_file:
        key = fdnDCIC.FDN_Key(keypairs_file, "default")
        connection = fdnDCIC.FDN_Connection(key)
        response = fdnDCIC.new_FDN(connection, 'software', sl_filtered)


def download_dockerinfo(docker_reponame, docker_version):
    """
    downloads a text file from a docker repo that contains software info
    """
    url = 'https://raw.githubusercontent.com/' + docker_reponame + '/' + docker_version + '/downloads.sh'
    downloaded_file_name = 'downloads.sh'
    wget.download(url, downloaded_file_name)
    return downloaded_file_name


def download_cwl(cwlfile, subdir=None, branch='dev', cwlrepo='https://raw.githubusercontent.com/4dn-dcic/pipelines-cwl', maindir='cwl_awsem'):
    """
    downloads cwl file from a cwl repo (default 4dn cwl repo).
    subdir and cwlfile can be e.g. subdir='repliseq', cwlfile='repliseq-parta.cwl'
    branch is the branch name of the cwl repo
    """
    if subdir:
        url = cwlrepo + '/' + branch + '/' + maindir + '/' + subdir + '/' + cwlfile
    else:
        url = cwlrepo + '/' + branch + '/' + maindir + '/' + cwlfile
    try:
        wget.download(url, cwlfile)
    except:
        print("failed to download cwl: url={}".format(url))
        raise

    return cwlfile


def cwlfile2wfdict(cwlfile, software_insert_jsonfile=None, cwl_subdir=None):
    cwl = create_cwl_from_file(cwlfile)

    if software_insert_jsonfile:
        with open(software_insert_jsonfile, 'r') as f:
            software_dict_list = json.load(f)
        sw_list = [Software(**_) for _ in software_dict_list]
    else:
        sw_list = None

    wf = Workflow(cwl_subdir=cwl_subdir).create_from_cwl(cwl, sw_list)
    return wf.as_dict()


def cwld3_2_cwlv1(cwld3file, cwlv1file):
    print("processing cwl %s" % cwld3file)
    cwl = create_cwl_from_file(cwld3file)
    if not cwl:
        print("Not writing v1 file")
        return 0
    if cwl.cwlVersion != 'draft-3':
        raise Exception("CWL version is not draft-3")
    cwldict = rdict(cwl)
    cwldict['cwlVersion'] = 'v1.0'
    for op in cwldict['outputs']:
        if 'source' in op:
            op['outputSource'] = op['source']
            op['outputSource'] = op['outputSource'].replace('.', '/')
            del(op['source'])
    for st in cwldict['steps']:
        for ip in st['inputs']:
            ip['id'] = ip['id'].replace('.', '/')
            if 'source' in ip:
                ip['source'] = ip['source'].replace('.', '/')
        for op in st['outputs']:
            op['id'] = op['id'].replace('.', '/')
        st['in'] = st['inputs']
        del(st['inputs'])
        st['out'] = st['outputs']
        del(st['outputs'])
    for op in cwldict['outputs']:
        if 'description' in op:
            op['doc'] = op['description']
            del(op['description'])
    for ip in cwldict['inputs']:
        if 'description' in ip:
            ip['doc'] = ip['description']
            del(ip['description'])
    with open(cwlv1file, 'w') as fw:
        print(json.dump(cwldict, fw, sort_keys=True, indent=4))


def cwlfile2wffile(cwlfile, wffile, software_insert_jsonfile=None, cwl_subdir=None):
    wf = cwlfile2wfdict(cwlfile, software_insert_jsonfile, cwl_subdir=cwl_subdir)
    with open(wffile, 'w') as fw:
        print(json.dump(wf, fw, sort_keys=True, indent=4))


def add_workflow_to_insert(workflow_insert_jsonfile, software_insert_jsonfile, cwlfile, subdir=None, branch='dev', cwlrepo='https://raw.githubusercontent.com/4dn-dcic/pipelines-cwl', maindir='cwl_awsem', post=False, keypairs_file=None):
    """
    downloads cwl file from cwl repo, adds software metadata link to it,
    and replaces the workflow insert json file by adding or updating (if exists) the workflow entry
    """
    cwlfile = download_cwl(cwlfile, subdir, branch, cwlrepo, maindir)
    wf = cwlfile2wfdict(cwlfile, software_insert_jsonfile, cwl_subdir=subdir)

    # if the uuid already exists in the insert, update that entry.
    # if not, add the new one.
    with open(workflow_insert_jsonfile, 'r') as f:
        d = json.load(f)
    existing = False
    for i, wf_old in enumerate(d):
        if wf['uuid'] == wf_old.get('uuid'):
            d[i] = wf
            existing = True
            break
        if wf['name'] == wf_old.get('name'):
            wf['uuid'] = wf_old.get('uuid')
            d[i] = wf
            existing = True
            break

    if not existing:
        d.append(wf)

    # overwrite the insert file
    with open(workflow_insert_jsonfile, 'w') as fw:
        json.dump(d, fw, sort_keys=True, indent=4)

    #post to 4dn data portal
    if post and keypairs_file:
        key = fdnDCIC.FDN_Key(keypairs_file, "default")
        connection = fdnDCIC.FDN_Connection(key)
        if existing:
            response = fdnDCIC.patch_FDN(wf['uuid'], connection, wf)
        else:
            response = fdnDCIC.new_FDN(connection, wf['uuid'], wf)


def add_repliseq_software(docker_version='v11'):
    """
    autogenrates software/workflow inserts from repli-seq pipeline e.g. v11 to inserts and prod-inserts

    WARNING: this function overwrites input insert files. 
    """
    add_software_to_insert('4dn-dcic/docker-4dn-repliseq', docker_version, 'src/encoded/tests/data/inserts/software.json')
    add_software_to_insert('4dn-dcic/docker-4dn-repliseq', docker_version, 'src/encoded/tests/data/prod-inserts/software.json')
    add_workflow_to_insert('src/encoded/tests/data/inserts/workflow.json', 'src/encoded/tests/data/inserts/software.json', 'repliseq-parta.cwl', 'repliseq', 'dev')
    add_workflow_to_insert('src/encoded/tests/data/prod-inserts/workflow.json', 'src/encoded/tests/data/prod-inserts/software.json', 'repliseq-parta.cwl', 'repliseq', 'dev')

