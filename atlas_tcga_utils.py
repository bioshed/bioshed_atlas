import os, sys, json
SCRIPT_DIR = str(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.join(SCRIPT_DIR))
import atlas_utils
sys.path.append('bioshed_utils/')
import quick_utils

def combine_all( base_dir ):
    """ Combines all manifest files into one.
    """
    DIRS = ['assay', 'disease', 'filetype', 'platform', 'tissue', 'project']
    COLS = ['filename', 'md5', 'project', 'assay','tissue','disease','species','platform','filetype']
    manifest_all = {}
    for DIR in DIRS:
        files_all = os.listdir(os.path.join(base_dir, DIR))
        manifest_files = list(filter(lambda x: x.endswith('.txt'), files_all))
        for mfile in manifest_files:
            category = DIR
            value = mfile.split('.')[-2]
            with open(os.path.join(base_dir, DIR,mfile),'r') as f:
                for r in f:
                    # print(r)
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
                        manifest_all[_id][category] = value
    # print(str(manifest_all))
    with open('manifest-all-gdc.txt', 'w') as fout:
        fout.write('id\tfilename\tmd5\tproject\tassay\ttissue\tdisease\tspecies\tplatform\tfiletype\n')
        for mid in list(manifest_all.keys()):
            row = mid+'\t'
            for COL in COLS:
                row += manifest_all[mid][COL]+'\t' if COL in manifest_all[mid] else '.\t'
            fout.write(row.rstrip(' \t')+'\n')
    return 'manifest-all-gdc.txt'

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
