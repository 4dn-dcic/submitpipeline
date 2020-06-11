import json
import yaml

# Field/attribute names ending with '__' (e.g. isFile__) are excluded on serialization.
# They are internal use only. (e.g. class to class conversion)


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
        self.name__ = id.strip('#')

        # type (as in cwl), isFile__, isArray__ (parsed)
        self._type = kwargs.get("type")
        self.isFile__ = False
        self.isArray__ = False
        if self._type:
            if "File" in self._type:
                self.isFile__ = True
            else:
                for t in self._type:
                    if isinstance(t, dict) and 'type' in t and 'items' in t:
                        if t.get('type') == 'array':
                            self.isArray__ = True
                            if t.get('items') == 'File':
                                self.isFile__ = True

        # source (as in cwl), source_step__, source_arg__ (parsed)
        if outputSource:
            self.outputSource = outputSource
            self.source_step__, self.source_arg__ = self.outputSource.strip('#').split('/')

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
        self.name__ = id.strip('#')

        # type (as in cwl), isFile__, isArray__ (parsed)
        self._type = kwargs.get("type")
        self.isFile__ = False
        self.isArray__ = False
        if self._type:
            if "File" in self._type:
                self.isFile__ = True
            else:
                for t in self._type:
                    if isinstance(t, dict) and 'type' in t and 'items' in t:
                        if t.get('type') == 'array':
                            self.isArray__ = True
                            if t.get('items') == 'File':
                                self.isFile__ = True

        # source (as in cwl), source_step__, source_arg__ (parsed)
        if source:
            self.source = source
            self.source_step__, self.source_arg__ = self.source.strip('#').split('.')

        self.fdn_format = fdn_format
        self.description = description
        self.fdn_output_type = fdn_output_type
        self.fdn_secondary_file_formats = fdn_secondary_file_formats


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
            self.name__ = id.strip('#')

        # type (as in cwl), isFile__, isArray__ (parsed)
        self._type = kwargs.get("type")
        self.isFile__ = False
        self.isArray__ = False
        if self._type:
            if "File" in self._type:
                self.isFile__ = True
            else:
                for t in self._type:
                    if isinstance(t, dict) and 'type' in t and 'items' in t:
                        if t.get('type') == 'array':
                            self.isArray__ = True
                            if t.get('items') == 'File':
                                self.isFile__ = True

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
            self.name__ = id.strip('#')

        # type (as in cwl), isFile__, isArray__ (parsed)
        self._type = kwargs.get("type")
        self.isFile__ = False
        self.isArray__ = False
        if self._type:
            if "File" in self._type:
                self.isFile__ = True
            else:
                for t in self._type:
                    if isinstance(t, dict) and 'type' in t and 'items' in t:
                        if t.get('type') == 'array':
                            self.isArray__ = True
                            if t.get('items') == 'File':
                                self.isFile__ = True

        self.default = default
        self.description = description
        self.fdn_format = fdn_format
        self.fdn_secondary_file_formats = fdn_secondary_file_formats


class CwlStepOutput (object):
    def __init__(self, id=None, fdn_format=None, fdn_type=None, fdn_cardinality=None, arg_name=None):
        """
        take in elements of a cwl's 'steps::outputs' field (dictionary) as kwargs
        """
        assert(id)

        # id (as in cwl), name, step_name__, arg_name (parsed)
        self.id = id
        self.name__ = id.strip('#')
        if '/' in self.name__:
            self.step_name__, self.arg_name = self.name__.split('/')
        else:
            self.step_name__ = None
            self.arg_name = self.name__
        if arg_name:
            self.arg_name = arg_name

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

        # id (as in cwl), name, step_name__, arg_name (parsed)
        self.id = id
        self.name__ = id.strip('#')
        self.step_name__, self.arg_name = self.name__.split('.')

        # 4dn tags
        self.fdn_format = fdn_format
        self.fdn_type = fdn_type
        self.fdn_cardinality = fdn_cardinality


class CwlStepInput (object):
    def __init__(self, id=None, source=None, fdn_format=None, fdn_type=None, fdn_cardinality=None,
                 arg_name=None):
        """
        take in elements of a cwl's 'steps::inputs' field (dictionary) as kwargs
        """
        assert(id)

        # id (as in cwl), name, step_name__, arg_name (parsed)
        self.id = id
        self.name__ = id.strip('#')
        if '/' in self.name__:
            self.step_name__, self.arg_name = self.name__.split('/')
        else:
            self.step_name__ = None
            self.arg_name = self.name__
        if arg_name:
            self.arg_name = arg_name

        # source (as in cwl), source_step__, source_arg__ (parsed)
        if source:
            self.source = source
            if len(self.source.strip('#').split('/')) == 2:
                self.source_step__, self.source_arg__ = self.source.strip('#').split('/')
            else:
                self.source_arg__ = self.source.strip('#')
                self.source_step__ = None
        else:
            self.source = None
            self.source_step__ = None
            self.source_arg__ = None

        # 4dn tags
        self.fdn_format = fdn_format
        self.fdn_type = fdn_type
        self.fdn_cardinality = fdn_cardinality


class CwlStepInputD3 (object):
    def __init__(self, id=None, source=None, fdn_format=None, fdn_type=None, fdn_cardinality=None):
        """
        take in elements of a cwl's 'steps::inputs' field (dictionary) as kwargs
        """
        assert(id)

        # id (as in cwl), name, step_name__, arg_name (parsed)
        self.id = id
        self.name__ = id.strip('#')
        self.step_name__, self.arg_name = self.name__.split('.')

        # source (as in cwl), source_step__, source_arg__ (parsed)
        if source:
            self.source = source
            if len(self.source.strip('#').split('.')) == 2:
                self.source_step__, self.source_arg__ = self.source.strip('#').split('.')
            else:
                self.source_arg__ = self.source.strip('#')
                self.source_step__ = None
        else:
            self.source = None
            self.source_step__ = None
            self.source_arg__ = None

        # 4dn tags
        self.fdn_format = fdn_format
        self.fdn_type = fdn_type
        self.fdn_cardinality = fdn_cardinality


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
            self.name__ = id.strip('#')
        self.run = run
        self.out = [CwlStepOutput(**_) if isinstance(_, dict) else CwlStepOutput(id=_) for _ in out]
        if isinstance(kwargs.get('in'), list):
            self._in = [CwlStepInput(**_) for _ in kwargs.get('in')]
        elif isinstance(kwargs.get('in'), dict):
            self._in = [CwlStepInput(id=k, **v) for k, v in kwargs.get('in').items()]
        if fdn_step_meta:
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
            self.name__ = id.strip('#')
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

        assert(self._class == 'Workflow')

        if self.cwlVersion == 'draft-3':
            self.inputs = [CwlInputD3(**_) for _ in inputs]
            self.outputs = [CwlOutputD3(**_) for _ in outputs]
            self.steps = [CwlStepD3(**_) for _ in steps]
        elif self.cwlVersion == 'v1.0':
            if isinstance(inputs, list):
                self.inputs = [CwlInput(**_) for _ in inputs]
            elif isinstance(inputs, dict):
                self.inputs = [CwlInput(id=k, **v) for k, v in inputs.items()]
            if isinstance(outputs, list):
                self.outputs = [CwlOutput(**_) for _ in outputs]
            elif isinstance(outputs, dict):
                self.outputs = [CwlOutput(id=k, **v) for k, v in outputs.items()]
            if isinstance(steps, list):
                self.steps = [CwlStep(**_) for _ in steps]
            elif isinstance(steps, dict):
                self.steps = [CwlStep(id=k, **v) for k, v in steps.items()]

        self.requirements = requirements
        if fdn_meta:
          self.fdn_meta = CwlFdnMeta(**fdn_meta)
        else:
          self.fdn_meta = None

 
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


def create_cwl_from_file(file):
    try:
        with open(file, 'r') as f:
            cwldict = json.load(f)
    except json.decoder.JSONDecodeError as e:
        print("not in a json format, trying yaml")
        try:
            with open(file, 'r') as f:
                 cwldict = yaml.load(f)
        except Exception as e2:
            raise Exception("Can't load CWL file %s: %s" % (file, str(e2)))
    except:
            raise Exception("Can't load CWL file %s: %s" % (file, str(e)))
    try:
        return Cwl(**cwldict)
    except AssertionError as e:
        print(e)
        return None


def rdict(x):
    """
    recursive conversion to dictionary
    converts objects in list members to dictionary recursively
    attributes beginning with '_' is converted to fields excluding the '_'
    """
    if isinstance(x, list):
        l = [rdict(y) for y in x]
        return l
    elif isinstance(x, dict):
        x2={}
        for k, v in x.items():
            if k.endswith('__'):
                continue
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
                if k.endswith('__'):  # internal use only
                    toremove.append(k)

            # remove items with a None value
            for k, v in d.items():
                if v is None:
                    toremove.append(k)
            for k in set(toremove):
                del(d[k])

            # go deep
            for k, v in d.items():
                d[k] = rdict(v)
            return d
        else:
            return x
