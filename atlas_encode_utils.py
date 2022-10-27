import sys, os, json
import pandas as pd
import atlas_utils
sys.path.append('bioshed_utils/')
import quick_utils

DEFAULT_SEARCH_FILE = "search_encode.txt"

def search_encode( args ):
    """ Entrypoint for an ENCODE search.
    $ bioshed search encode <searchterms>
    Examples
    $ bioshed search encode breast cancer rna-seq
    $ bioshed search encode --tissue heart --assay chip-seq

    searchterms: search terms input by user
    ---
    results: data frame of results

    >>> search_encode( dict(searchterms='breast cancer rna-seq'))
    ''
    >>> search_encode( dict(searchterms='--tissue heart --assay chip-seq'))
    ''

    Prints number of experiment datasets found and where results are output to (search_encode.txt).
    Outputs search results to search_encode.txt (tab-delimited text)
    This search results file is fed into download relevant files with the following command:

    $ bioshed download encode

    You can further filter search_encode.txt before download by adding search terms:

    $ bioshed download encode --filetype fastq

    By default, this will download to the current folder. You can specify a relative path to download or a remote cloud bucket to download to with the --output parameter.

    $ bioshed download encode --output <local_outdir>
    $ bioshed download encode --output s3://my/s3/folder

    NOTE: You MUST have a bioshed_encode.txt file before you run bioshed download.

    You can also specify a bioshed-formatted ENCODE results file to download files:

    $ bioshed download encode newsearch_results.txt

    For help with anything, type:
    $ bioshed search encode --help
    $ bioshed download encode --help
    """
    # dictionary of search terms: {"general": "...", "tissue": "...", "celltype": "..."...}
    search_dict = atlas_utils.parse_search_terms( args['searchterms'] )

    URL_BASE = 'https://encodeproject.org/search/'
    # start with search url base and build it according to search terms
    url_search_string = ''
    for category, terms in search_dict.items():
        url_search_string = combine_search_strings(url_search_string, convert_to_search_string( dict(terms=terms, category=category)))
    return encode_search_url( dict(url='/search/{}'.format(url_search_string), searchtype='full', returntype='full'))


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
    [NOTE] search is limited to 50000 results

    >>> encode_search_url(dict(url="experiments/ENCSR000BDC/", searchtype="experiment", returntype="raw"))
    ''
    >>> encode_search_url(dict(url="experiments/ENCSR000BDC/", searchtype="experiment", returntype="file"))
    ''
    """
    returntype = args['returntype'] if 'returntype' in args else 'full'
    searchtype = args['searchtype'] if 'searchtype' in args else 'full'
    search_url = 'https://www.encodeproject.org/{}&limit=50000'.format(str(args['url']).lstrip('/'))
    print('GET request: {}'.format(search_url))
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
                tbl['file'].append(list(map(lambda f: f["@id"], fullexpt["files"])) if "files" in fullexpt else [])
        print('Number of experiment datasets found: {}'.format(str(len(tbl['experiment']))))
        print('Number of assays found: {}'.format(str(len(list(set(tbl['assay']))))))
        print('Number of cell types found: {}'.format(str(len(list(set(tbl['celltype']))))))
        print('Number of total files found: {}'.format(str(sum([len(e) for e in tbl['file']]))))
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
    ---
    expts: list of experiments in URL format (/experiments/ENCSR000ACHE/,...)

    https://www.encodeproject.org/experiments/ENCSR000AHE/
    """
    results = args['results']
    expts = []
    for fullexpt in results["@graph"]:
        expts.append(str(fullexpt["@id"]))
    return expts

def get_files_from_encode_json( args ):
    """ Get files from an ENCODE results JSON
    results: results JSON from a raw ENCODE search
    searchtype: the type of raw ENCODE search passed in
    cloud: which remote cloud (aws/s3 or google/gcp or microsoft/azure)
    ---
    relevant_files: list of S3 file URIs

    """
    results = args['results']
    searchtype = args['searchtype'] if 'searchtype' in args else ''
    cloud = args['cloud'] if 'cloud' in args else 's3'

    relevant_files = []
    if searchtype=='experiment':
        # original search was an experiment
        for f in results["files"]:
            if cloud in ['s3','aws','amazon']:
                relevant_files.append(f["s3_uri"])
    return relevant_files

########################## HELPER FUNCTIONS ############################

def print_dataframe( df ):
    """ Print pandas dataframe to command line
    """
    pd.set_option('display.expand_frame_repr', False)
    pd.set_option('display.max_colwidth', 50)
    pd.set_option('display.precision', 2)
    print(df)
    return

def convert_to_search_string( args ):
    """ Given a category and search terms, outputs an updated url search string

    terms: breast cancer
    category: general
    ---
    urlstring: search string

    Example: 'breast cancer', 'general' => ?type=Experiment&searchTerm=breast+cancer
    Example: 'colon cancer', 'disease' => ...
    NOTE: there may be overlapping terms, which is why original search string is passed in (may be modified in-place).
    """
    terms = args['terms']
    category = args['category']
    pre_search_file = 'files/search_encode_{}.txt'.format(str(category).lower())
    search_string = ''

    if os.path.exists(pre_search_file):
        df = pd.read_csv(pre_search_file, sep='\t')
        if 'link' in df.columns and 'ID' in df.columns:
            pd_query = df[df['ID']==terms]['link'] # df.query('ID=={}'.format(terms))['link']
            if len(pd_query) > 0:
                search_string = pd_query.values[0]
    if search_string == '':
        search_string = '?type=Experiment&searchTerm={}'.format(terms.lower().replace(' ','+'))
    return search_string

def combine_search_strings( ss1, ss2 ):
    """ Combine two search strings into a single ENCODE search string.
    This removes any redundant search terms.
    """
    if 'type=Experiment' in ss1 and 'type=Experiment' in ss2:
        ss2 = ss2.replace('type=Experiment', '')
    if 'type=Biosample' in ss1 and 'type=Biosample' in ss2:
        ss2 = ss2.replace('type=Biosample', '')

    if ss1 != '' and ss2 != '':
        return '{}&{}'.format(ss1,ss2.lstrip('?'))
    elif ss1 == '' and ss2 != '':
        return ss2
    elif ss1 != '' and ss2 == '':
        return ss1
    else:
        return ''

########################## DEPRECATED ############################

def search_encode_general( args ):
    """ Search ENCODE for datasets using general search terms.
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
