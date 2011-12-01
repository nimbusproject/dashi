
def test_dict_from_dotted():
    from dashi.bootstrap import _dict_from_dotted

    key = "my.very.handsome.key"
    val = "tacos"

    test_dict = _dict_from_dotted(key, val)

    assert test_dict.my.very.handsome.key == val
    assert test_dict['my']['very']['handsome']['key'] == val

def test_parse_argv():
    from dashi.bootstrap import _parse_argv

    config_file = "path/to/config.yml"
    non_yml_config = "path/to/bad/config"
    option1_key = "option1"
    option1_val = "quesedillas"
    option1 = "--%s=%s" % (option1_key, option1_val)
    option2_key = "option2.thing"
    option2_val = "tortas"
    option2 = "--%s %s" % (option2_key, option2_val)
    flag1_key = "chorizo"
    flag1 = "--%s" % flag1_key
    flag2_key = "nachos"
    flag2 = "--%s" % flag2_key

    argv = ["service", config_file, option1, flag1, option2, flag2]

    cfg, cfg_files = _parse_argv(argv)

    assert cfg_files[0] == config_file
    assert cfg[option1_key] == option1_val
    assert cfg['option2']['thing'] == option2_val
    assert cfg[flag1_key] == True
    assert cfg[flag2_key] == True
    assert non_yml_config not in cfg_files
