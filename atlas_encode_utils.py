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
    ---
    results:

    >>> search_encode( dict(tissue='breast cancer'))
    ''
    """
    search_args = ''
    for arg in args:
        search_args += args[arg].replace(' ','%20') if arg in args else ''

    search_url = 'https://www.encodeproject.org/search/?type=Experiment&searchTerm={}'.format(search_args)
    results_raw = quick_utils.get_request( dict(url=search_url, type='application/json'))
    return results_raw
