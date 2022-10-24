import sys, os, json
import pandas as pd
sys.path.append('bioshed_utils/')
import quick_utils

DEFAULT_SEARCH_FILE = "search_encode.txt"

def print_dataframe( df ):
    """ Print pandas dataframe to command line
    """
    pd.set_option('display.expand_frame_repr', False)
    pd.set_option('display.max_colwidth', 50)
    pd.set_option('display.precision', 2)
    print(df)
    return

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
    >>> search_encode( dict(tissue='breast cancer', returntype='assay'))
    ''
    """
    returntype = args['returntype'] if 'returntype' in args else 'full'
    search_args = ''
    for arg in args:
        if arg not in ['returntype']:
            search_args += args[arg].replace(' ','%20') if arg in args else ''
    search_url = '/search/?type=Experiment&searchTerm={}'.format(search_args)
    results = encode_search_url( dict(url=search_url, searchtype='full', returntype=returntype))
    print_dataframe( results )
    return results

def encode_search_url( args ):
    """ Searches ENCODE by a URL suffix.
    https://www.encodeproject.org/<URL_SUFFIX>

    url: url suffix to search
    searchtype: 'full' (full search), 'experiment' (search within an experiment),...
    returntype: 'full' (default), 'raw', 'file', 'experiment', 'celltype', 'assay', 'platform',...
    ---
    results:

    - By default, this returns the full table of info, and also writes results to a default file.
    - We can also return the raw JSON result with 'raw' as the return type.
    - If the return type is a specific column of the full table, then a
      "joined" table is returned where the key is the requested return type.
        ex: returntype='file'
        FILE    EXPERIMENT  CELLTYPE    ASSAY   ACCESSION
        ... (one row per file)

    >>> encode_search_url(dict(url="experiments/ENCSR000BDC/", searchtype="experiment", returntype="raw"))
    ''
    >>> encode_search_url(dict(url="experiments/ENCSR000BDC/", searchtype="experiment", returntype="file"))
    ''
    """
    returntype = args['returntype'] if 'returntype' in args else 'full'
    searchtype = args['searchtype'] if 'searchtype' in args else 'full'
    search_url = 'https://www.encodeproject.org/' + str(args['url']).lstrip('/')
    results_raw = quick_utils.get_request( dict(url=search_url, type='application/json'))
    if returntype.lower() == 'raw':
        return results_raw
    elif searchtype.lower() == 'full':
        return get_full_info_from_encode_json( dict(results=results_raw, sortby=returntype))
    elif returntype.lower() == 'experiment':
        return get_experiments_from_encode_json( dict(results=results_raw))
    elif returntype.lower() == 'file':
        return get_files_from_encode_json( dict(results=results_raw, searchtype=searchtype))
    else:
        return results_raw

def get_full_info_from_encode_json( args ):
    """ Get experiments with full info from an ENCODE search JSON.
    results: results_raw
    sortby: which column to sort by - full/experiment (default), file, assay, etc...
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
    sortby = args['sortby'] if 'sortby' in args else 'experiment'
    returntype = args['returntype'] if 'returntype' in args else 'pandas'

    if sortby in ['full', 'experiment']:
        tbl = {"experiment": [], "assay": [], "celltype": [], "species": [], "accession": [], "file": []}
        for fullexpt in results["@graph"]:
            if "@id" in fullexpt:
                tbl['experiment'].append(str(fullexpt["@id"]))
                tbl['assay'].append(str(fullexpt["assay_term_name"]) if "assay_term_name" in fullexpt else '')
                tbl['celltype'].append(str(fullexpt["biosample_ontology"]["term_name"]) if "biosample_ontology" in fullexpt and "term_name" in fullexpt["biosample_ontology"] else '')
                tbl['species'].append(' '.join(str(fullexpt["biosample_summary"]).split(' ')[0:2]) if "biosample_summary" in fullexpt else '')
                tbl['accession'].append(list(fullexpt["dbxrefs"]) if "dbxrefs" in fullexpt else [])
                tbl['file'].append(list(map(lambda f: f["@id"], fullexpt["files"])))
    elif sortby == 'assay':
        # create joined table for gathering info
        jtbl = {}  # key is assay
        for fullexpt in results["@graph"]:
            if "@id" in fullexpt and "assay_term_name" in fullexpt:
                assay = fullexpt["assay_term_name"]
                if assay not in jtbl:
                    jtbl[assay] = {"experiment": [], "celltype": [], "species": [], "accession": [], "file": []}
                jtbl[assay]["experiment"].append(str(fullexpt["@id"]))
                if "biosample_ontology" in fullexpt and "term_name" in fullexpt["biosample_ontology"]:
                    jtbl[assay]['celltype'].append(str(fullexpt["biosample_ontology"]["term_name"]))
                if "biosample_summary" in fullexpt:
                    jtbl[assay]['species'].append(' '.join(str(fullexpt["biosample_summary"]).split(' ')[0:2]))
                if "dbxrefs" in fullexpt:
                    jtbl[assay]['accession'].append(list(fullexpt["dbxrefs"]))
                if "files" in fullexpt:
                    jtbl[assay]['file'].append(list(map(lambda f: f["@id"], fullexpt["files"])))
        # create flat table for output
        tbl = {"assay": [], "experiment": [], "celltype": [], "species": [], "accession": [], "file": []} # full table
        for k, v in jtbl.items():
            tbl["assay"].append(k)
            tbl["experiment"].append(v["experiment"] if "experiment" in v else '')
            tbl["celltype"].append(v["celltype"] if "celltype" in v else '')
            tbl["species"].append(v["species"] if "species" in v else '')
            tbl["accession"].append(v["accession"] if "accession" in v else [])
            tbl["file"].append(v["file"] if "file" in v else [])

    if returntype in ['pandas', 'dataframe']:
        tbl_df = pd.DataFrame(tbl)
        tbl_df.index.name = 'index'
        tbl_df.to_csv(DEFAULT_SEARCH_FILE, sep='\t')
        return tbl_df
    else:
        return tbl

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
