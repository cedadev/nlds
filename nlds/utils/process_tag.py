def process_tag(tag):
    """Process a tag in string format into dictionary format"""
    # try:
    if True:
        tag_dict = {}
        # strip "{" "}" symbolsfirst
        tag_list = (tag.replace("{", "").replace("}", "")
                    ).split(",")
        for tag_i in tag_list:
            tag_kv = tag_i.split(":")
            if len(tag_kv) < 2:
                continue
            tag_dict[tag_kv[0]] = tag_kv[1]
    # except: # what exceptions might be raised here?
    #     raise ValueError
    return tag_dict