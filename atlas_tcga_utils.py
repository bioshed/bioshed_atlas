import os, sys, json
import pandas as pd
import gzip
SCRIPT_DIR = str(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.join(SCRIPT_DIR))
import atlas_utils
sys.path.append('bioshed_utils/')
import quick_utils
import aws_s3_utils

GENERIC_TERMS = ["cancer", "tumor", "tumour", "dataset"]
DEFAULT_SEARCH_FILE = "search_gdc.txt"

def search_gdc( args ):
    """ Entrypoint for a TCGA or GDC search.
    In general, we recommend using GDC, as this repository includes TCGA and several other consortiums.

    $ bioshed search tcga <searchterms>
    -OR-
    $ bioshed search gdc <searchterms>

    Examples
    $ bioshed search gdc breast cancer variants
    $ bioshed search gdc --tissue heart --assay rna-seq

    searchterms: search terms input by user
    ---
    search_results: data frame of results

    >>> search_gdc( dict(searchterms='breast cancer variants'))
    ''
    >>> search_gdc( dict(searchterms='--tissue heart --assay rna-seq'))
    ''
    >>> search_gdc( dict(searchterms='--assay single cell --tissue brain'))
    ''

    Prints number of experiment datasets found and where results are output to (search_gdc.txt).
    Outputs search results to search_gdc.txt (tab-delimited text)
    This search results file is fed into download relevant files with the following command:

    $ bioshed download gdc
    -OR-
    $ bioshed download tcga

    You can further filter search_gdc.txt before download by adding search terms:

    $ bioshed download gdc --filetype txt

    By default, this will download to the current folder. You can specify a relative path to download or a remote cloud bucket to download to with the --output parameter.

    $ bioshed download gdc --output <local_outdir>
    $ bioshed download gdc --output s3://my/s3/folder

    NOTE: You MUST have a bioshed_gdc.txt file before you run bioshed download.

    You can also specify a bioshed-formatted GDC results file to download files:

    $ bioshed download gdc newsearch_results.txt

    For help with anything, type:
    $ bioshed search gdc --help
    $ bioshed download gdc --help
    """
    MANIFEST_FILE = os.path.join(SCRIPT_DIR, "files/gdc/manifest-all-gdc.txt.gz")
    CATEGORIES_FILE = os.path.join(SCRIPT_DIR, 'files/gdc/categories-all-gdc.txt')
    print('Using manifest file: {}'.format(MANIFEST_FILE))
    print('Using category file: {}'.format(CATEGORIES_FILE))
    search_results = {}
    search_string = args['searchterms'] if 'searchterms' in args else ''
    # dictionary of search terms: {"general": "...", "tissue": "...", "celltype": "..."...}
    search_dict = atlas_utils.parse_search_terms( search_string ) if search_string != '' else {}
    if search_dict == {} or 'help' in search_dict:
        print_gdc_help()
    else:
        search_dict = convert_general_terms( search_dict, CATEGORIES_FILE )
        print('Search dictionary: {}'.format(str(search_dict)))
        search_results = get_manifest_rows( search_dict, search_string, MANIFEST_FILE )
    return search_results


def convert_general_terms( search_dict, CATEGORIES_FILE ):
    """ Converts general search terms to specific categories.

    search_dict: search dictionary
    CATEGORIES_FILE: tab-delimited file with category in 1st column and possible search terms in 2nd column
    ---
    search_dict: updated search dict

    Example:
    {"general": "breast cancer rna-seq"} => {"tissue": "breast", "assay": "rna-seq"}
    """
    categories = {}
    if "general" in search_dict:
        with open(CATEGORIES_FILE,'r') as f:
            for r in f:
                rt = r.strip().split('\t')
                category = rt[0]
                terms = rt[1]
                categories[category] = terms

        general_terms = search_dict["general"]
        # just use simple word-based search for now
        general_terms_split = quick_utils.format_type(general_terms, 'list')
        for gterm in general_terms_split:
            for k, v in categories.items():
                if quick_utils.quick_format(gterm) in v and gterm not in GENERIC_TERMS:
                    search_dict = atlas_utils.add_term(search_dict, k, gterm)
    return search_dict


def get_manifest_rows( search_dict, search_string, MANIFEST_FILE ):
    """
    Gets rows from GDC-formatted manifest file that match search terms.
    Valid search terms are:
    --tissue / --assay / --celltype / --disease / --filetype / --platform / --species

    search_dict: dictionary of search terms: {"general": "...", "tissue": "...", "assay": "..."}
    search_string: original search string
    ---
    results: data frame of results
    (out): filtered GDC-format manifest file
    """

    df = pd.read_csv(MANIFEST_FILE, compression='gzip', sep='\t')
    columns = list(df.columns)
    # look for each search term within the corresp category column in the manifest data frame
    for k, v in search_dict.items():
        if k == 'celltype' or (k == 'disease' and ('tumor' in v or 'tumour' in v or 'cancer' in v)):
            k = 'tissue'
        if k in columns:
            # special case where user wants a list of valid search terms, e.g. search gdc --filetype
            if len(search_dict) == 1 and v.strip() == "":
                unique_terms = str('_'.join(list(set(list(df[k]))))).split('_')
                print('Valid search terms for category {}'.format(k))
                for unique_term in unique_terms:
                    if unique_term != '.':
                        print('\t{}'.format(unique_term))
                return {}
            # otherwise we do a search
            terms = quick_utils.format_type(v, 'list')
            for t in terms:
                t = quick_utils.quick_format(t)
                if t not in GENERIC_TERMS:
                    df = df.loc[df[k].str.contains(t, case=False)]
    # write filtered data frame to output file for download
    with open(DEFAULT_SEARCH_FILE,'w') as fout:
        fout.write('# bioshed search gdc {}\n'.format(search_string))
    df.index.name = 'index'
    df.to_csv(DEFAULT_SEARCH_FILE, sep='\t', mode='a')

    print(df)
    print('')
    print('Number of files found: {}'.format(str(len(df['filename']))))
    print('Number of unique assays found: {}'.format(str(len(list(set(df['assay']))))))
    print('Number of tissues-of-origin found: {}'.format(str(len(list(set(df['tissue']))))))

    print('Search results written to {}.'.format(DEFAULT_SEARCH_FILE))
    print('Type "bioshed download gdc" to download data files or "bioshed download gdc --list" for file info before downloading.')
    return df

def download_gdc( args ):
    """ Entrypoint for an GDC download.
    Assumes that search_gdc() has already been run, so that a search_gdc.txt file exists.
    User can refine search.

    downloadstr: download string passed in, which can include the following:

    --help (help menu)
    --list : list files, but do not download (like a dryrun)
    --update : update only - do not overwrite existing files
    --input <file name>
    --output <output directory>
    --filetype <refine by file type(s) - space delimited>
    --assay <refine by assay>
    --id <refine by ID>
    --index <refine by index>
    --tissue <refine by tissue of origin>

    Multiple ids or indexes can be specified by either comma or space delimiting:
    --id 34005 34006
    --id 34005,34006

    [TODO] Clean up documentation and write tests

    [NOTE] Current format for search_gdc.txt is:
    id      filename        md5     project assay   tissue  disease species platform        filetype
    6fd3fe64-23ba-4db3-8315-1867bdec277d    b7274dab-7650-4de3-91a1-7accf806e870.mirbase21.isoforms.quantification.txt      a773004e175527ad668a8db92f0c4e80        tcga    transcriptome-mirna-seq-small-rnaseq    heart   .       human   .       filetype-txt
    [NOTE] '|'.join learned from:
    https://stackoverflow.com/questions/26577516/how-to-test-if-a-string-contains-one-of-the-substrings-in-a-list-in-pandas
    """
    BASE_S3_DIR = "s3://tcga-2-open/"
    BASE_HTTPS_DIR = "https://api.gdc.cancer.gov/data/"
    INFO_COLUMNS = ['id', 'assay', 'tissue']
    dd = atlas_utils.parse_search_terms( args['downloadstr'] if 'downloadstr' in args else '')
    infile = dd['input'] if 'input' in dd else DEFAULT_SEARCH_FILE
    outdir = dd['output'] if 'output' in dd else str(os.getcwd())
    filetype = dd['filetype'] if 'filetype' in dd else ''
    assay = dd['assay'] if 'assay' in dd else ''
    _id = dd['id'] if 'id' in dd else ''
    tissue = dd['tissue'] if 'tissue' in dd else ''
    index = dd['index'] if 'index' in dd else ''
    filename = dd['filename'] if 'filename' in dd else ''
    listonly = 'True' if 'list' in dd else ''
    updateonly = 'True' if 'update' in dd else ''
    outfiles = []
    downloaded_files = []
    annotation_info = []
    ANNOTATION_INFO_FILE = os.path.join(os.getcwd(),'annotation_gdc.txt')
    original_search_command = ''

    if 'help' in dd:
        print_gdc_help()
    elif os.path.exists(infile):
        # get comment line (first line), if there
        with open(infile,'r') as f:
            r = f.readline()
            if r[0] == '#':
                original_search_command = r.strip()
        # get table                        
        df = pd.read_csv(infile, sep='\t', comment='#')
        # print(df)
        if assay != '':
            df = df.loc[df['assay'].str.contains(assay, case=False)]
        if _id != '':
            _id = quick_utils.format_type(_id, 'list')
            df = df.loc[df['id'].str.contains('|'.join(_id), case=False)]
        if index != '':
            index = quick_utils.format_type(index, 'list')
            df = df.loc[df['index'].astype(str).str.contains('|'.join(index))]
        if tissue != '':
            df = df.loc[df['tissue'].str.contains(tissue, case=False)]
        if filetype != '':
            df = df.loc[df['filetype'].str.contains(filetype, case=False)]
        if filename != '':
            df = df.loc[df['filename'].str.contains(filename, case=False)]

        # generate list of files
        if quick_utils.cloud_initialized(dict(cloud='aws')):
            df['filepath'] = BASE_S3_DIR + df['id'] + '/' + df['filename']
        else:
            df['filepath'] = BASE_HTTPS_DIR + df['id']
        
        if listonly == 'True':
            # list files, but do not download
            print(df[['filename', 'tissue', 'assay']])
            with open(ANNOTATION_INFO_FILE,'w') as fout:
                if original_search_command != '':
                    fout.write(original_search_command+'\n')
                fout.write('FILE\tEXPERIMENT\tASSAY\tCELLTYPE\tSPECIES\n')
                for idx, row in df.iterrows():
                    fout.write('{}\t{}\t{}\t{}\t{}\n'.format(str(row['filename']), '.', str(row['assay']).replace('_','/'), str(row['tissue']).replace('_','/'), 'human'))

        else:
            outfiles = list(df['filepath'])
            # download files
            if outdir.startswith('s3') and len(outfiles) > 0 and outfiles[0].startswith('s3'):
                # s3 file transfer
                downloaded_files = aws_s3_utils.transfer_file_s3( dict(path=outfiles, outpath=outdir, overwrite='False' if updateonly=='True' else 'True'))
            elif not outdir.startswith('s3') and len(outfiles) > 0 and outfiles[0].startswith('s3'):
                downloaded_files = aws_s3_utils.download_file_s3( dict(path=outfiles, localdir=outdir, overwrite='False' if updateonly=='True' else 'True'))
            elif not outdir.startswith('s3') and len(outfiles) > 0 and outfiles[0].startswith('http'):
                # http download to local
                for idx, row in df.iterrows():
                    outfile = row['filepath']
                    outfile_name = row['filename']
                    print('Downloading {}'.format(outfile_name))
                    # get http(s) file
                    _response = quick_utils.get_request( dict(url=outfile))
                    # write to local file
                    open(os.path.join(outdir, quick_utils.get_file_only(outfile_name)), 'wb').write(_response.content)
                    # save list of downloaded files
                    downloaded_files.append(os.path.join(outdir, quick_utils.get_file_only(outfile_name)))


            # print annotation info
            with open(ANNOTATION_INFO_FILE,'w') as fout:
                if original_search_command != '':
                    fout.write(original_search_command+'\n')
                fout.write('FILE\tEXPERIMENT\tASSAY\tCELLTYPE\tSPECIES\n')
                for idx, row in df.iterrows():
                    fout.write('{}\t{}\t{}\t{}\t{}\n'.format(str(row['filename']), '.', str(row['assay']).replace('_','/'), str(row['tissue']).replace('_','/'), 'human'))

    else:
        print('File {} does not exist. Please first run "bioshed search gdc"'.format(infile))
    return downloaded_files


def combine_all( base_dir ):
    """ Combines all manifest files into one tab-delimited manifest.
    See COLS variable for column names and order.

    base_dir: base directory where GDC-format manifest files reside
    ---
    (out): full manifest file
    (out): category list file

    """
    DIRS = ['assay', 'disease', 'filetype', 'platform', 'tissue', 'project']
    COLS = ['filename', 'md5', 'project', 'assay','tissue','disease','species','platform','filetype']
    _MANIFEST_FILE = 'manifest-all-gdc.txt'
    _CATEGORIES_FILE = 'categories-all-gdc.txt'
    manifest_all = {}
    categories = {}

    for DIR in DIRS:
        files_all = os.listdir(os.path.join(base_dir, DIR))
        manifest_files = list(filter(lambda x: x.endswith('.txt'), files_all))
        for mfile in manifest_files:
            category = DIR
            value = mfile.split('.')[-2]
            # add search term to category list
            if category not in categories:
                categories[category] = []
            categories[category].append(value)
            # add to manifest dictionary
            with open(os.path.join(base_dir, DIR,mfile),'r') as f:
                for r in f:
                    rt = r.strip().split('\t')
                    if rt[0] != 'id':
                        _id = rt[0]
                        _fn = rt[1]
                        _md5 = rt[2]
                        if _id not in manifest_all:
                            manifest_all[_id] = {}
                            manifest_all[_id]['filename'] = _fn
                            manifest_all[_id]['md5'] = _md5
                            manifest_all[_id]['species'] = 'human'
                            manifest_all[_id]['project'] = 'other'
                        if category not in manifest_all[_id]:
                            manifest_all[_id][category] = value
                        else:
                            manifest_all[_id][category] += '_'+value

    # write to manifest file
    with open(_MANIFEST_FILE, 'w') as fout:
        fout.write('id\tfilename\tmd5\tproject\tassay\ttissue\tdisease\tspecies\tplatform\tfiletype\n')
        for mid in list(manifest_all.keys()):
            row = mid+'\t'
            for COL in COLS:
                row += manifest_all[mid][COL]+'\t' if COL in manifest_all[mid] else '.\t'
            fout.write(row.rstrip(' \t')+'\n')
    # write to category file
    with open(_CATEGORIES_FILE,'w') as fout:
        for category in list(categories.keys()):
            fout.write('{}\t{}\n'.format(category, str(categories[category])))

    print(str(categories))
    return _MANIFEST_FILE

def gdc_run_all( manifest_list_file ):
    """ Runs gdc_manifest_full() for all files
    """
    base_dir = quick_utils.get_file_folder(manifest_list_file)
    manifest_full_files = []
    with open(manifest_list_file,'r') as f:
        for r in f:
            rt = r.strip().split('\t')
            manifest_file = rt[0]
            json_file = rt[1]
            fout = gdc_manifest_full( os.path.join(base_dir, json_file), os.path.join(base_dir, manifest_file) )
            manifest_full_files.append(fout)
    print(str(manifest_full_files))
    return

def gdc_manifest_full( gdc_json_file, gdc_manifest_file ):
    """ Given GDC JSON metadata file and GDC manifest file from
    Genomic Data Commons, creates a full manifest file with data format
    and data type info.
    https://portal.gdc.cancer.gov/repository
    Select a primary site, then click on Manifest and click on JSON.
    """
    gdc_json_list = quick_utils.getJSON( gdc_json_file )
    gdc_json_dict = {} # file_name: [data_format, data_category]
    gdc_manifest_out = gdc_manifest_file[:-4]+'.full.txt'
    for gdc_entry in gdc_json_list:
        data_format = gdc_entry["data_format"]
        file_name = gdc_entry["file_name"]
        data_category = gdc_entry["data_category"]
        gdc_json_dict[file_name] = [data_format, data_category]
    with open(gdc_manifest_file,'r') as f, open(gdc_manifest_out,'w') as fout:
        fout.write('id\tfilename\tmd5\tdata_format\tdata_category\n')
        for r in f:
            rt = r.strip().split('\t')
            _id = rt[0]
            _fn = rt[1]
            _md5 = rt[2]
            if _id not in ['id'] and _fn in gdc_json_dict:
                data_format = gdc_json_dict[_fn][0]
                data_category = gdc_json_dict[_fn][1]
                fout.write('\t'.join([_id, _fn, _md5, data_format, data_category])+'\n')
    return gdc_manifest_out


def gdc_json_to_txt( gdc_json_file ):
    """ Converts JSON metadata downloaded from Genomic Data Commons
    to a tab-delimited text.
    https://portal.gdc.cancer.gov/repository
    then click on JSON.

    Format: (example)
        [{
      "data_format": "SVS",
      "cases": [
        {
          "case_id": "a9fe64a9-6d22-4e9f-96f3-f16af7d298f8",
          "project": {
            "project_id": "TCGA-UVM"
          }
        }
      ],
      "access": "open",
      "file_name": "TCGA-V4-A9EA-01Z-00-DX1.DB8360B6-2BF1-4538-AF2C-29EB7186946E.svs",
      "data_category": "Biospecimen",
      "file_size": 1865302441
    },{
      "data_format": "IDAT",
      "cases": [
        {
          "case_id": "15d19ccc-52b8-41f6-b1c1-2cc55691aed5",
          "project": {
            "project_id": "TCGA-UVM"
          }
        }
      ],
      "access": "open",
      "file_name": "b7723059-b441-428c-bb7d-c69ddb22a886_noid_Grn.idat",
      "data_category": "DNA Methylation",
      "file_size": 8095272
    }
    """
    fout_name = gdc_json_file[:-5]+'.csv'
    fout = open(fout_name,'w')
    fout.write('file_name\tdata_category\tdata_format\tcase_ids\tproject_ids\n')
    gdc_json_list = quick_utils.getJSON( gdc_json_file )
    for gdc_entry in gdc_json_list:
        data_format = gdc_entry["data_format"]
        if "cases" in gdc_entry and len(gdc_entry["cases"]) > 0:
            case_ids = ','.join(list(map(lambda x: x["case_id"] if "case_id" in x else '', gdc_entry["cases"])))
            project_ids = ','.join(list(map(lambda x: x["project"]["project_id"] if "project" in x else '', gdc_entry["cases"])))
        else:
            case_ids = ''
            project_ids = ''
        file_name = gdc_entry["file_name"]
        data_category = gdc_entry["data_category"]
        fout.write('\t'.join([file_name, data_category, data_format, case_ids, project_ids])+'\n')
    return fout_name


def print_gdc_help():
    """ Prints the command-line help menu when a user types a wrong input or
    when a user just types "bioshed search gdc".
    """
    print('\nWelcome to GDC/TCGA dataset search and download, powered by BioShed Atlas.\n')

    print('------------------------------------------------------------')
    print('BioShed GDC SEARCH')
    print('------------------------------------------------------------\n')
    print('Usage:')
    print('\tbioshed search gdc <SEARCH>')
    print('\tbioshed search tcga <SEARCH>\n')

    print('Search Examples:')
    print('\tGeneral search:\t\t$ bioshed search tcga breast cancer variant call')
    print('\tSearch by category:\t$ bioshed search gdc --tissue heart --assay rna-seq\n')

    print('Search categories:')
    print('\t--tissue\tTissue where DNA/RNA/protein material was extracted.')
    print('\t--assay\tAssay type performed.') 
    print('\t--disease\tDiagnosed disease type.')
    print('\t--filetype\tType of data file.')
    print('\t--platform\tInstrument platform used.')
    print('\t--species\tHuman, mouse, etc.\n')

    print('To list search terms (example):\n')
    print('\t$ bioshed search gdc --tissue')
    print('')
    print('BioShed will write SEARCH results to a file "search_gdc.txt" in the current directory.\n')

    print('------------------------------------------------------------')
    print('BioShed GDC DOWNLOAD')
    print('------------------------------------------------------------\n')
    print('Usage: To download data files listed in "search_gdc.txt", type\n')
    print('\t$ bioshed download gdc\n')

    print('Further refine the files you want using any of the search categories. For example\n')
    print('\t$ bioshed download gdc --filetype fastq')
    print('\t$ bioshed download gdc --id 6fd3fe64-23ba-4db3-8315-1867bdec277d\n')
    
    print('You can list the files before downloading them, by typing:\n')
    print('\t$ bioshed download gdc --list\n')
    
    print('You can specify a different output directory, including an AWS S3 remote folder:\n')
    print('\t$ bioshed download gdc --output s3://my/output/folder\n')
    
    print('By default, existing files in the output directory will be overwritten. To download only new files:\n')
    print('\t$ bioshed download gdc --update\n')
    print('Successful download will also generate an associated annotation file "annotation_gdc.txt".\n')
    return
