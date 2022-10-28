import sys, os, json
import pandas as pd
import atlas_utils
sys.path.append('bioshed_utils/')
import quick_utils
import aws_s3_utils

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
    >>> search_encode( dict(searchterms='--assay single cell --tissue heart'))
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
    URL_BASE = 'https://encodeproject.org/search/'
    url_search_string = ''
    search_results = {}
    # dictionary of search terms: {"general": "...", "tissue": "...", "celltype": "..."...}
    search_dict = atlas_utils.parse_search_terms( args['searchterms'] ) if ('searchterms' in args and args['searchterms'] != '') else {}
    if search_dict == {} or 'help' in search_dict:
        print_encode_help()
    else:
        # start with search url base and build it according to search terms
        for category, terms in search_dict.items():
            url_search_string = combine_search_strings(url_search_string, convert_to_search_string( dict(terms=terms, category=category)))
        if url_search_string != '' and '&searchTerm=&' not in url_search_string:
            search_results = encode_search_url( dict(url='/search/{}'.format(url_search_string), searchtype='full', returntype='full'))
    return search_results

def encode_search_url( args ):
    """ Searches ENCODE by a URL suffix.
    https://www.encodeproject.org/<URL_SUFFIX>

    url: url suffix to search
    searchtype: 'full' (full search), 'experiment' (search within an experiment), 'file' (search within a file)
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
    if 'url' not in args:
        print('ERROR: You need to specify a URL.')
        return {}
    elif str(args['url']).lstrip('/').startswith('search'):
        search_url = 'https://www.encodeproject.org/{}&limit=50000'.format(str(args['url']).lstrip('/'))
    else:
        search_url = 'https://www.encodeproject.org/{}'.format(str(args['url']).lstrip('/'))

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
        print('Search results written to {}.'.format(DEFAULT_SEARCH_FILE))
        print('Type "bioshed download encode" to download data files or "bioshed download encode --list" for file info before downloading.')
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

    [TODO] for 'file' searchtype, can get HTTPS object link via 'cloud_metadata' key.

    """
    results = args['results']
    searchtype = args['searchtype'] if 'searchtype' in args else ''
    cloud = args['cloud'] if 'cloud' in args else 's3'

    relevant_files = []
    if searchtype in ['experiment']:
        # original search was an experiment
        for f in results["files"]:
            if cloud in ['s3','aws','amazon']:
                relevant_files.append(f["s3_uri"])
    elif searchtype in ['file']:
        relevant_files.append(results["s3_uri"])
    return relevant_files

def download_encode( args ):
    """ Entrypoint for an ENCODE download.
    Assumes that search_encode() has already been run, so that a search_encode.txt file exists.
    User can refine search.
    downloadstr: download string passed in, which can include the following:

    --help (help menu)
    --list : list files, but do not download (like a dryrun)
    --input <file name>
    --output <output directory>
    --filetype <refine by file type(s) - space delimited>
    --assay <refine by assay>
    --species <refine by species>
    --experiment <refine by experiment ID>
    --celltype <refine by cell type>

    [NOTE] Current format for search_encode.txt is:
    index	experiment	assay	celltype	species	accession	file
    0	/experiments/ENCSR718YPN/	single-nucleus ATAC-seq	heart left ventricle	Homo sapiens	[]	['/files/ENCFF804ONU/', '/files/ENCFF393IGF/',...]

    [NOTE] Use str.contains:  df2 = df.loc[df['celltype'].str.contains('heart', case=False)]
    [DONE] s3 file transfer function in aws_s3_utils
    """
    INFO_COLUMNS = ['experiment', 'assay', 'celltype', 'species']
    dd = atlas_utils.parse_search_terms( args['downloadstr'] if 'downloadstr' in args else '')
    infile = dd['input'] if 'input' in dd else 'search_encode.txt'
    outdir = dd['output'] if 'output' in dd else str(os.getcwd())
    filetype = dd['filetype'] if 'filetype' in dd else ''
    assay = dd['filetype'] if 'filetype' in dd else ''
    species = dd['species'] if 'species' in dd else ''
    experiment = dd['experiment'] if 'experiment' in dd else ''
    celltype = dd['celltype'] if 'celltype' in dd else ''
    listonly = 'True' if 'list' in dd else ''
    outfiles = []
    outfiles_info = {}
    downloaded_files = []

    if 'help' in dd:
        print_encode_help()
    elif os.path.exists(infile):
        df = pd.read_csv(infile, sep='\t')
        if assay != '':
            df = df.loc[df['assay'].str.lower().contains(assay, case=False)]
        if species != '':
            df = df.loc[df['species'].str.lower().contains(species, case=False)]
        if experiment != '':
            df = df.loc[df['experiment'].str.lower().contains(experiment, case=False)]
        if celltype != '':
            df = df.loc[df['celltype'].str.lower().contains(celltype, case=False)]

        experiment_urls = list(df['experiment'])
        for e_url in experiment_urls:
            # get paths of all files for each experiment
            outfiles_new = encode_search_url( dict(url=e_url, searchtype='experiment', returntype='file'))
            outfiles += outfiles_new
            for outfile in outfiles_new:
                outfiles_info[outfile] = '; '.join(list(map(str, df.loc[df['experiment']==e_url][INFO_COLUMNS].values.flatten().tolist())))

        if filetype!='':
            # if --filetype filter is specified
            ftypes = filetype.split(' ')
            for ftype in ftypes:
                outfiles = list(filter(lambda f: ftype in f, outfiles))
                removed_files = list(filter(lambda f: ftype not in f, outfiles))
                for removed_file in removed_files:
                    removed_info = outfiles_info.pop(removed_file, 'not found')

        if listonly == 'True':
            # list files, but do not download
            print('FILE\tINFO (EXPT; ASSAY; CELLTYPE; SPECIES)')
            for outfile, outfile_info in outfiles_info.items():
                print('{}\t{}'.format(quick_utils.get_file_only(outfile), str(outfile_info)))
        else:
            # download files
            if outdir.startswith('s3') and len(outfiles) > 0 and outfiles[0].startswith('s3'):
                # s3 file transfer
                downloaded_files = aws_s3_utils.transfer_file_s3( dict(path=outfiles, outpath=outdir))
            else:
                downloaded_files = aws_s3_utils.download_file_s3( dict(path=outfiles, localdir=outdir))
    else:
        print('File {} does not exist. Please first run "bioshed search encode"'.format(infile))
    return downloaded_files


########################## HELPER FUNCTIONS ############################

def print_dataframe( df ):
    """ Print pandas dataframe to command line
    """
    pd.set_option('display.expand_frame_repr', False)
    pd.set_option('display.max_colwidth', 50)
    pd.set_option('display.precision', 2)
    print(df)
    return

def print_encode_help():
    """ Prints the command-line help menu when a user types a wrong input or
    when a user just types "bioshed search encode".
    """
    print('Welcome to ENCODE dataset search and download, powered by BioShed Atlas.')
    print('')
    print('Usage: bioshed search encode <SEARCH>\n')
    print('Examples:\n')
    print('\t$ bioshed search encode breast cancer rna-seq')
    print('\t$ bioshed search encode --tissue heart --assay chip-seq')
    print('')
    print('--tissue and --assay are examples of search categories used to speed up searches.\n')
    print('Valid search categories are:')
    print('--tissue / --assay / --assaytarget / --celltype / --disease / --genome / --filetype / --platform / --species')
    print('')
    print('To see a list of valid search terms for a category, you can just type, for example:\n')
    print('\t$ bioshed search encode --filetype')
    print('')
    print('Once you have performed a search, bioshed will write dataset results to search_encode.txt in the current directory.')
    print('To then download those dataset files, you can then run: \n')
    print('\t$ bioshed download encode')
    print('')
    print('Further refine the files you want using any of the search categories. For example\n')
    print('\t$ bioshed download encode --filetype fastq')
    print('')
    print('You can list the files before downloading them, by typing:\n')
    print('\t$ bioshed download encode --list')
    print('')
    print('You can specify a different output directory, including an AWS S3 remote folder:\n')
    print('\t$ bioshed download encode --output s3://my/output/folder')
    print('')
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

    try:
        if os.path.exists(pre_search_file):
            df = pd.read_csv(pre_search_file, sep='\t')
            if category != '' and terms == '':
                # if category list is asked for:
                print('You need to provide a search term for category --{}. Valid search terms are: {}\n'.format(category, list(df['ID'])))
            elif 'link' in df.columns and 'ID' in df.columns:
                pd_query = df[df['ID']==terms]['link'] # df.query('ID=={}'.format(terms))['link']
                if len(pd_query) > 0:
                    search_string = pd_query.values[0]
        elif not os.path.exists(pre_search_file) and category not in ['general', ''] and terms == '':
            # incorrect category used
            raise ValueError('ERROR: Invalid search category {}. Type "bioshed search encode" to see valid search categories, or just search without categories.'.format(category))

        if search_string == '':
            search_string = '?type=Experiment&searchTerm={}'.format(terms.lower().replace(' ','+'))
        return search_string
    except ValueError as e:
        print(e)
        return ''

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
