import sys, os, json
sys.path.append('bioshed_utils/')
import quick_utils

def search_encode( args ):
    """ Search ENCODE for datasets.
    tissue: same as organ...
    celltype: ...
    assaytype: ...
    species: ...
    filetype: ...
    platform: Illumina etc..
    generic: generic search...
    returntype: 'raw' (default), 'file', 'experiment'
    ---
    results:

    https://www.encodeproject.org/search/?type=Experiment&searchTerm=breast+cancer

    >>> search_encode( dict(tissue='breast cancer'))
    ''
    >>> search_encode( dict(tissue='breast cancer', returntype='experiment'))
    ''
    """
    returntype = args['returntype']
    search_args = ''
    for arg in args:
        if arg not in ['returntype']:
            search_args += args[arg].replace(' ','%20') if arg in args else ''
    search_url = '/search/?type=Experiment&searchTerm={}'.format(search_args)
    return encode_search_url( dict(url=search_url, returntype=returntype) )

def encode_search_url( args ):
    """ Searches ENCODE by a URL suffix.
    https://www.encodeproject.org/<URL_SUFFIX>

    url: url suffix to search
    returntype: 'raw' (default), 'file', 'experiment'
    ---
    results:

    """
    returntype = args['returntype']
    search_url = 'https://www.encodeproject.org/' + str(args['url']).lstrip('/')
    results_raw = quick_utils.get_request( dict(url=search_url, type='application/json'))
    if returntype.lower() == 'raw':
        return results_raw
    elif returntype.lower() == 'experiment':
        return encode_get_experiments( dict(results=results_raw))
    else:
        return results_raw

def encode_get_experiments( args ):
    """ Get experiment IDs from an ENCODE search.
    results: results from a raw search.

    https://www.encodeproject.org/experiments/ENCSR000AHE/
    """
    results = args['results']
    expts = []
    for fullexpt in results["@graph"]:
        expts.append(str(fullexpt["@id"]))
    return expts
