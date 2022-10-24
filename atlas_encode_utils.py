import sys, os, json
import pandas as pd
sys.path.append('bioshed_utils/')
import quick_utils

DEFAULT_SEARCH_FILE = "search_encode.txt"

def search_encode( args ):
    """ Search ENCODE for datasets.
    tissue: same as organ...
    celltype: ...
    assaytype: ...
    species: ...
    filetype: ...
    platform: Illumina etc..
    generic: generic search...
    returntype: 'raw' (default), 'full', 'file', 'experiment', 'assay', 'tissue',...
    ---
    results:

    EXAMPLE: search_encode( dict(tissue='breast cancer'))

    https://www.encodeproject.org/search/?type=Experiment&searchTerm=breast+cancer

    >>> search_encode( dict(tissue='breast cancer', returntype='full'))
    ''
    >>> search_encode( dict(tissue='breast cancer', returntype='experiment'))
    ''
    """
    returntype = args['returntype'] if 'returntype' in args else 'raw'
    search_args = ''
    for arg in args:
        if arg not in ['returntype']:
            search_args += args[arg].replace(' ','%20') if arg in args else ''
    search_url = '/search/?type=Experiment&searchTerm={}'.format(search_args)
    return encode_search_url( dict(url=search_url, searchtype='raw', returntype=returntype) )

def encode_search_url( args ):
    """ Searches ENCODE by a URL suffix.
    https://www.encodeproject.org/<URL_SUFFIX>

    url: url suffix to search
    searchtype: 'raw', 'experiment',...
    returntype: 'raw' (default), 'full', 'file', 'experiment', 'tissue', 'platform',...
    ---
    results:

    >>> encode_search_url(dict(url="experiments/ENCSR000BDC/", searchtype="experiment", returntype="raw"))
    ''
    >>> encode_search_url(dict(url="experiments/ENCSR000BDC/", searchtype="experiment", returntype="file"))
    ''
    """
    returntype = args['returntype'] if 'returntype' in args else 'raw'
    searchtype = args['searchtype'] if 'searchtype' in args else ''
    search_url = 'https://www.encodeproject.org/' + str(args['url']).lstrip('/')
    results_raw = quick_utils.get_request( dict(url=search_url, type='application/json'))
    if returntype.lower() == 'raw':
        return results_raw
    elif returntype.lower() == 'experiment':
        return get_experiments_from_encode_json( dict(results=results_raw))
    elif returntype.lower() == 'file':
        return get_files_from_encode_json( dict(results=results_raw, searchtype=searchtype))
    elif returntype.lower() == 'full':
        return get_full_info_from_encode_json( dict(results=results_raw, searchtype=searchtype))
    else:
        return results_raw

def get_full_info_from_encode_json( args ):
    """ Get experiments with full info from an ENCODE search JSON.
    results: results_raw
    searchtype: searchtype
    returntype: JSON or dataframe (default)
    ---
    fullinfo: JSON or dataframe
    DEFAULT_SEARCH_FILE (outfile): table

    https://www.encodeproject.org/search/?type=Experiment&searchTerm=breast+cancer
    FOR EACH EXPERIMENT:
        "assay_term_name": "ChIP-seq",
        "assay_title": "Control ChIP-seq",
        "biosample_ontology": {
            "term_name": "MCF-7"
        },
        "biosample_summary": "Homo sapiens MCF-7",
        "dbxrefs": [
            "GEO:GSM1010854", <--
            "UCSC-ENCODE-hg19:wgEncodeEH003428"
        ],

    """
    results = args['results']
    returntype = args['returntype'] if 'returntype' in args else 'pandas'
    expts = {"experiment": [], "assay": [], "celltype": [], "species": [], "accession": []}
    for fullexpt in results["@graph"]:
        if "@id" in fullexpt:
            expts['experiment'].append(str(fullexpt["@id"]))
            expts['assay'].append(str(fullexpt["assay_term_name"]) if "assay_term_name" in fullexpt else '')
            expts['celltype'].append(str(fullexpt["biosample_ontology"]["term_name"]) if "biosample_ontology" in fullexpt and "term_name" in fullexpt["biosample_ontology"] else '')
            expts['species'].append(' '.join(str(fullexpt["biosample_summary"]).split(' ')[0:2]) if "biosample_summary" in fullexpt else '')
            expts['accession'].append(list(fullexpt["dbxrefs"]) if "dbxrefs" in fullexpt else [])
    expts_df = pd.DataFrame(expts)
    expts_df.index.name = 'index'
    expts_df.to_csv(DEFAULT_SEARCH_FILE, sep='\t')
    if returntype in ['pandas', 'dataframe']:
        return expts_df
    else:
        return expts

def print_dataframe( df ):
    """ Print pandas dataframe to command line
    """
    pd.set_option('display.expand_frame_repr', False)
    pd.set_option('display.max_colwidth', 50)
    pd.set_option('display.precision', 2)
    print(expts_df)
    return

def get_experiments_from_encode_json( args ):
    """ Get experiment IDs from an ENCODE search JSON.
    results: results JSON from a raw search.

    https://www.encodeproject.org/experiments/ENCSR000AHE/
    """
    results = args['results']
    expts = []
    for fullexpt in results["@graph"]:
        expts.append(str(fullexpt["@id"]))
    return expts

def get_files_from_encode_json( args ):
    """ Get files from an ENCODE results JSON
    results: results_raw
    searchtype: searchtype
    ---
    relevant_files

    """
    results = args['results']
    searchtype = args['searchtype'] if 'searchtype' in args else ''
    relevant_files = []
    if searchtype=='experiment':
        # original search was an experiment
        for f in results["files"]:
            relevant_files.append(f["s3_uri"])
    return relevant_files
