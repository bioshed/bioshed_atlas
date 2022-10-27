def parse_search_terms( search_string ):
    """ Takes string of search terms and returns categorized dictionary.

    Example: "breast cancer --assay rna-seq" => {"general": "breast cancer", "assay": "rna-seq"}

    """
    search_dict = {}
    category = 'general'
    search_list = search_string.split(' ')
    while len(search_list) > 0:
        s = search_list[0]
        if s[0:2] != '--':
            if category not in search_dict:
                search_dict[category] = ''
            search_dict[category] += s+' '
        else:
            category = s[2:]
        search_list = search_list[1:]
    for k,v in search_dict.items():
        search_dict[k] = v.strip()

    return search_dict
