"""
Interfacing with oAuth token files and token database
"""

def get_tokens_from_file( token_file ):
    # Set up API access
    if token_file.endswith('yaml'):
        #YAML file
        tokens = yaml.safe_load(open(token_file))
    elif token_file.endswith('py'):
        #.py file -- surely there is a better way to do this
        tokens = {}
        for line in open(token_file):
            k,v = [x.strip() for x in line.split("=")]
            tokens[k] = v[1:-1]
    else:
        raise "Unrecognized token file type -- please use a .yaml or .py file following the examples"
    return tokens
