{
    "class": "Workflow", 
    "cwlVersion": "v1.0", 
    "inputs": [
        {
            "id": "#input_file", 
            "type": [
                "null", 
                "File"
            ]
        }
    ], 
    "outputs": [
        {
            "id": "#validatefiles_report", 
            "outputSource": "#validatefiles/report", 
            "type": [
                "null", 
                "File"
            ]
        }, 
        {
            "id": "#md5_report", 
            "outputSource": "#md5/report", 
            "type": [
                "null", 
                "File"
            ]
        }
    ], 
    "requirements": [
        {
            "class": "InlineJavascriptRequirement"
        }
    ], 
    "steps": [
        {
            "id": "#md5", 
            "in": [
                {
                    "id": "#md5/input_file", 
                    "source": "#input_file"
                }
            ], 
            "out": [
                {
                    "id": "#md5/report"
                }
            ], 
            "run": "md5.cwl"
        }, 
        {
            "id": "#validatefiles", 
            "in": [
                {
                    "id": "#validatefiles/input_file", 
                    "source": "#input_file"
                }
            ], 
            "out": [
                {
                    "id": "#validatefiles/report"
                }
            ], 
            "run": "validate.cwl"
        }
    ]
}